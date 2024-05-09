from collections import defaultdict
from typing import List, Tuple
import uuid
from tqdm import tqdm
import asyncio
import json
import datetime

from llama_index.vector_stores.qdrant import QdrantVectorStore
from llama_index.core import StorageContext, VectorStoreIndex
from llama_index.core.schema import (
    BaseNode,
    Document
)
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference
from llama_index.core.node_parser import SentenceSplitter
import llama_index.core.instrumentation as instrument
from llama_index.core.ingestion import run_transformations
from qdrant_client import QdrantClient

from utils.clustering import get_clusters
from utils.process_jats import JatsXMLParser
from utils.splade_embedding import SpladeEmbeddingsInference
from settings import Settings
from utils.utils import BaseNodeTypeEnum, setup_logger, download_s3_file

dispatcher = instrument.get_dispatcher(__name__)
logger = setup_logger("DatabaseVectorsEngine")


class DatabaseVectorsEngine:  
    def sparse_doc_vectors(
            self,
            texts: List[str],
        ) -> Tuple[List[List[int]], List[List[float]]]:
        """
        Computes vectors from logits and attention mask using ReLU, log, and max operations.

        Args:
            texts (List[str]): A list of strings representing the input text data.

        Returns:
            Tuple[List[List[int]], List[List[float]]]: A tuple containing two lists.
            The first list is a list of lists, where each sublist represents the indices of the input text data.
            The second list is a list of lists, where each sublist represents the corresponding vectors derived
            from the input text data.
        """
        splade_embeddings = self.splade_model.get_text_embedding_batch(texts)
        indices = [[entry.get('index') for entry in sublist] for sublist in splade_embeddings]
        vectors = [[entry.get('value') for entry in sublist] for sublist in splade_embeddings]

        assert len(indices) == len(vectors)
        return indices, vectors
    
    async def parse_clean_fulltext(
            self,
            fulltext: str,
            name: str,
            split_depth: int) -> dict:
        """
        Asynchronously parses the full text of a document, cleans it, and organizes its content
        based on specified criteria. The function filters out certain sections, then groups
        paragraphs by a prefix derived from their IDs up to a given depth.

        Parameters:
            fulltext (str): The XML data of the fulltext to be parsed.
            name (str): The name identifier for the parser, typically related to the document type or source.
            split_depth (int): The depth to which section IDs should be split to group paragraphs.

        Returns:
            dict: A dictionary where keys are the section ID prefixes and values are lists of paragraphs
                  from sections that match the grouping criteria.

        Example Usage:
            result = await obj.parse_clean_fulltext("<xml>...</xml>", "DocumentName", 2)
        """
        jatParser = JatsXMLParser(name=name, xml_data=fulltext)
        parsed_details = jatParser.parse_root_node()

        to_be_processed_sections = [
            section for section in parsed_details.get('body_sections', [])
            if section.get('title') and section['title'] not in ["Title", "Abstract"]
        ]

        grouped_ids = defaultdict(list)
        for section in to_be_processed_sections:
            id_value = section.get('id', '')
            prefix = '.'.join(id_value.split('.')[:split_depth]) if '.' in id_value else id_value
            paragraph_texts = [
                content.get('text') for content in section.get('contents', []) 
                if content.get('tag') == "p"
            ]
            grouped_ids[prefix].extend(paragraph_texts)

        return grouped_ids
    
    async def calculate_embeddings(
            self,
            nodes: List[BaseNode]):
        """
        Asynchronously calculates text embeddings for a list of nodes using specified models.

        This function supports dense embeddings.

        Parameters:
            nodes (List[BaseNode]): A list of nodes for which embeddings will be calculated.

        Returns:
            Tuple[Optional[Dict[str, any]], Optional[Dict[str, any]]]: A tuple containing two dictionaries:
                - The first dictionary maps node IDs to their dense embeddings (if calculated).
                - The second dictionary maps node IDs to their SPLADE embeddings (if calculated).
                Both elements in the tuple will be None if their respective calculations are not performed.

        Usage:
            # Assuming 'node_list' is pre-defined and both embedding flags are true
            dense_emb = await calculate_embeddings(node_list)
        """
        id_to_dense_embedding = {}
        dense_embeddings = await self.embed_model.aget_text_embedding_batch(
            [node.get_content(metadata_mode="embed") for node in nodes], show_progress=True
            )
        id_to_dense_embedding = {
            node.id_: dense_embedding for node, dense_embedding in zip(nodes, dense_embeddings)
        }
        return id_to_dense_embedding
    
    async def generate_children_nodes(
            self,
            s3_object: str,
            pubmedid: int
    ):
        """
        Asynchronously generates children nodes from a specified S3 object's text content by parsing it
        and applying transformations.

        This function downloads text content from an S3 bucket, parses the text to extract meaningful
        sections, and then applies transformations to generate child nodes.

        Parameters:
            s3_object (str): The S3 object key from which the text content is downloaded.

        Returns:
            Dict[str, List[Document]]: A dictionary mapping sections of the parsed text to lists of
            Document objects representing children nodes.

        Usage:
            # Assuming 's3_key' is predefined and valid
            children_dict = await self.generate_children_nodes(s3_key)
        """
        fulltext_content = download_s3_file(self.s3_bucket, s3_object='bulk/' + s3_object)
        file_name = s3_object.split("/")[-1]
        cur_children_dict = defaultdict(list)

        if fulltext_content:
            parsed_fulltext = await self.parse_clean_fulltext(
                fulltext=fulltext_content,
                name=file_name,
                split_depth=self.split_depth)
            self.parsed_full_txt_json[pubmedid] = json.dumps(parsed_fulltext)
            for k, values in parsed_fulltext.items():
                base_nodes: List[Document] = [Document(text=each_value) for each_value in values if each_value.strip()]
                        
                cur_children_dict[k] = run_transformations(
                    base_nodes, 
                    transformations=[
                        SentenceSplitter(chunk_size=self.child_chunk_size, chunk_overlap=self.child_chunk_overlap)
                    ],
                    in_place=False)
        else:
            self.fulltext_not_found.append(file_name)
        return cur_children_dict
     
    #@dispatcher.span
    async def ainsert_single_record(
            self,
            pubmedid: int,
            abstract: str,
            **kwargs
        ) -> None:
        """
        Asynchronously inserts a single record into the vector datastore.

        Args:
            pubmedid (int): A unique identifier for the record.
            abstract (str): A concise summary of the content of the record.
            split_depth (int): The depth at which to split the content of the record.
            **kwargs: Additional keyword arguments that may be used to customize the insertion process.

        Raises:
            asyncio.TimeoutError: If an insertion task does not complete within the expected time.
            ConnectionError: If there is a failure in connecting to the datastore.

        Returns:
            None: This function does not return any value but completes the insertion of the record.

        Example of usage:
            await ainsert_single_record(pubmedid=123, abstract="Example abstract")
        """
        parent_base_nodes: List[Document] = [Document(text=abstract)]
        parent_nodes = run_transformations(
            nodes=parent_base_nodes,
            transformations=[
                SentenceSplitter(chunk_size=self.parent_chunk_size, chunk_overlap=self.parent_chunk_overlap)
            ],
            in_place=False
        )
        parent_id = parent_nodes[0].id_
        logger.info(f"ainsert_single_record. parent_id: {parent_id}. parent_nodes_count: {len(parent_nodes)}")
        cur_children_dict = defaultdict(list)

        #if fulltext exist - parse fulltext
        if kwargs.get("fulltext_to_be_parsed"):
            cur_children_dict = await self.generate_children_nodes(
                pubmedid=pubmedid,
                s3_object=kwargs.get("fulltext_s3_loc")
            )
        
        #prepare all the parent nodes and children nodes(if any)
        children_nodes =  [item for sublist in cur_children_dict.values() for item in sublist]
        all_cur_nodes = parent_nodes + children_nodes

        logger.info(f"ainsert_single_record. parent_id: {parent_id}. children_nodes_count: {len(children_nodes)}")
        logger.info(f"ainsert_single_record. parent_id: {parent_id}. all_cur_nodes: {len(all_cur_nodes)}")
        
        #call the embedding and splade embedding apis       
        dense_emb = await self.calculate_embeddings(nodes=all_cur_nodes)
        logger.info(f"ainsert_single_record. parent_id: {parent_id}. dense_emb length: {len(dense_emb)}")
        
        #prepare the tree here
        cur_level = self.tree_depth - 1
        nodes_to_be_added = []
        while cur_level >= 0:
            #set parent node
            if cur_level == 1:
                for p_node in parent_nodes:
                    cur_node = p_node
                    cur_node.metadata["level"] = 1
                    cur_node.metadata["node_type"] = BaseNodeTypeEnum.PARENT.value
                    nodes_to_be_added.append(cur_node)
                logger.info(f"ainsert_single_record. parent_id: {parent_id}. parent nodes processed to be added: {len(nodes_to_be_added)}")
            #set children nodes
            elif cur_level == 0:
                if len(children_nodes) > 0:
                    #cluster create 
                    for k, children_section_nodes in cur_children_dict.items():                 
                        children_section_nodes_ids = [node.id_ for node in children_section_nodes]
                        cur_id_to_dense_embedding = {k: v for k, v in dense_emb.items() if k in children_section_nodes_ids}

                        if len(children_section_nodes):
                            nodes_per_cluster = get_clusters(
                                children_section_nodes,
                                cur_id_to_dense_embedding
                            )
                        
                            for cluster, summary_doc in zip(nodes_per_cluster, children_section_nodes):
                                current_cluster_id = str(uuid.uuid4())
                                for cur_node in cluster:
                                    cur_node.metadata["level"] = 0
                                    cur_node.metadata["node_type"] = BaseNodeTypeEnum.CHILD.value
                                    cur_node.metadata["cluster_id"] = current_cluster_id
                                    nodes_to_be_added.append(cur_node)
                logger.info(f"ainsert_single_record. parent_id: {parent_id}. children nodes processed to be added: {len(nodes_to_be_added)}")
            cur_level = cur_level - 1

        #metadata update
        keys_to_update = ["title", "publicationDate", "year", "authors", "references", "identifiers"]
        for node in nodes_to_be_added:
            for key in keys_to_update:
                node.metadata[key] = kwargs.get(key)

        #metadata update for the nodes
        excluded_keys = ["pubmedid", "abstract", "level", "node_type", "cluster_id", "parent_id"] + keys_to_update
        for node in nodes_to_be_added:
            node.metadata["pubmedid"] = pubmedid
            node.metadata["abstract"] = abstract 
            node.metadata["parent_id"] = parent_id 
            node.embedding = dense_emb[node.id_]
            #node.splade_embedding = splade_emb[node.id_]
            for key in excluded_keys:
                node.excluded_embed_metadata_keys.append(key)
                node.excluded_llm_metadata_keys.append(key)

        #ready for the insertion to vectorDB
        self.index.insert_nodes(nodes_to_be_added)

    async def process_batch_records(self, records: list[defaultdict]) -> None:
        """
        Asynchronously processes a batch of records by inserting each record into a datastore.

        Each record in the batch is inserted by calling the `ainsert_single_record` method, 
        with concurrency limited by `num_workers`. This method efficiently handles
        multiple insertions by ensuring that no more than `num_workers` insert operations
        are running at the same time.

        Parameters:
            records (list[defaultdict]): A list of dictionaries, each representing a record to be inserted. 
            Each record should provide the necessary keys used by the `ainsert_single_record` method.

        Returns:
            None: This function does not return any value but completes the insertion of all records.

        Example of `records` parameter:
            records = [
                {"id": "123", "abstract": "Example abstract", "title": "Example Title", ...},
                {"id": "124", "abstract": "Another abstract", "title": "Another Title", ...}
            ]

        Raises:
            asyncio.TimeoutError: If an insertion task does not complete within the expected time.
            ConnectionError: If there is a failure in connecting to the datastore.
        """
        jobs = []
        for each_record in records:           
            jobs.append(
                self.ainsert_single_record(
                    each_record.get("id"),
                    each_record.get("abstract"),
                    title=each_record.get("title"),
                    publicationDate=each_record.get("publicationDate", "1200-05-02"),
                    year=each_record.get("year"),
                    authors=each_record.get("authors", []),
                    references=each_record.get("references", {}),
                    identifiers=each_record.get("identifiers", {}),
                    fulltext_s3_loc=each_record.get("fulltext_s3_loc", ""),
                    fulltext_to_be_parsed=each_record.get("fulltext_to_be_parsed", False)
                )
            )

        lock = asyncio.Semaphore(self.num_workers)
        # run the jobs while limiting the number of concurrent jobs to num_workers
        for job in jobs:
            async with lock:
                await job

    async def batch_process_records_to_vectors(self, records, batch_size=10000):
        """
        Processes records in batches asynchronously and collects the results.

        This method divides the input dictionary of records into batches of a specified
        size and processes each batch asynchronously. It utilizes concurrency to
        potentially speed up the processing of large volumes of data.

        Args:
            records (dict): A dictionary of records where the key is an identifier
                and the value is the data to be processed.
            batch_size (int): The number of records to process in each batch. Default is 10000.

        Returns:
            list: A list of results from processing each batch of records.

        Raises:
            Exception: If an error occurs during batch processing.
        """
        keys = list(records.keys())
        total_batches = (len(keys) + batch_size - 1) // batch_size
        final_results = []

        for i in tqdm(range(total_batches), desc="Processing batches"):
            start_index = i * batch_size
            end_index = start_index + batch_size
            batch_keys = keys[start_index:end_index]

            batch_data = [records[key] for key in batch_keys]
            result = await self.process_batch_records(batch_data)
            final_results.append(result)

        #dump the important results for future analytics
        timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        results_to_dump = {
            "full_details_not_found": self.fulltext_not_found,
            "parsed_fulltext_records": self.parsed_full_txt_json
        }
        with open(f"output_run_details/{timestamp}.json", 'w') as file:
            json.dump(results_to_dump, file, indent=4)

        return final_results

    def __init__(self,
                settings: Settings):
        
        self.settings = settings
        self.num_workers = 4

        self.s3_bucket  = self.settings.jatsparser.bucket_name
        self.split_depth = self.settings.jatsparser.split_depth
        
        self.parent_chunk_size = self.settings.d2vengine.parent_chunk_size
        self.parent_chunk_overlap = self.settings.d2vengine.parent_chunk_overlap
        self.child_chunk_size = self.settings.d2vengine.child_chunk_size
        self.child_chunk_overlap = self.settings.d2vengine.child_chunk_overlap
        self.tree_depth = self.settings.d2vengine.tree_depth

        self.text_embedding_url = self.settings.embedding.api_url
        self.text_embedding_token = self.settings.embedding.api_key.get_secret_value()
        self.text_embedding_batch_size = self.settings.embedding.embed_batch_size

        self.qdrant_url_address = self.settings.qdrant.api_url
        self.qdrant_url_port = self.settings.qdrant.api_port
        self.qdrant_collection_name = self.settings.qdrant.collection_name
        self.qdrant_api_key = self.settings.qdrant.api_key.get_secret_value()

        self.fulltext_not_found = []
        self.parsed_full_txt_json = {}
        self.embed_model = TextEmbeddingsInference(
            model_name="",
            base_url=self.text_embedding_url,
            auth_token=self.text_embedding_token,
            timeout=60,
            embed_batch_size=self.text_embedding_batch_size)
        
        self.splade_model = SpladeEmbeddingsInference(
            model_name="",
            base_url=self.settings.spladedoc.api_url,
            auth_token=self.settings.spladedoc.api_key.get_secret_value(),
            timeout=60,
            embed_batch_size=self.settings.spladedoc.embed_batch_size)

        self.client = QdrantClient(
            url=self.qdrant_url_address, 
            port=None,
            api_key=self.qdrant_api_key,
            https=True
            )   

        self.vector_store = QdrantVectorStore(
            client=self.client,
            collection_name=self.qdrant_collection_name, 
            sparse_doc_fn=self.sparse_doc_vectors,
            enable_hybrid=True,
            )
        
        self.storage_context = StorageContext.from_defaults(vector_store=self.vector_store)
        self.index = VectorStoreIndex(
            storage_context=self.storage_context,
            embed_model = self.embed_model,
            nodes =[]
        )   