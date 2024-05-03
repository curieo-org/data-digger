from collections import defaultdict
from typing import List
import uuid
from tqdm import tqdm

from llama_index.vector_stores.qdrant import QdrantVectorStore
from llama_index.core import StorageContext, ServiceContext, VectorStoreIndex
from llama_index.core.schema import (
    BaseNode,
    NodeWithScore,
    TextNode,
    Document
)
from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models
import asyncio
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference
from llama_index.core.node_parser import SentenceSplitter
import llama_index.core.instrumentation as instrument
from llama_index.core.ingestion import run_transformations

from utils.clustering import get_clusters
from utils.process_jats import JatsXMLParser
from utils.splade_embedding import SpladeEmbeddingsInference
from settings import Settings
from utils.utils import BaseNodeTypeEnum, setup_logger, download_s3_file

dispatcher = instrument.get_dispatcher(__name__)
logger = setup_logger("DatabaseVectorsEngine")


class DatabaseVectorsEngine:
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
            nodes: List[BaseNode],
            do_embedding: bool,
            do_splade: bool):
        """
        Asynchronously calculates text embeddings for a list of nodes using specified models.

        This function supports both dense and sparse embeddings and will execute them
        based on the flags provided. If a flag is set to True, the corresponding embedding
        type is calculated.

        Parameters:
            nodes (List[BaseNode]): A list of nodes for which embeddings will be calculated.
            do_embedding (bool): Flag to indicate whether to calculate dense embeddings.
            do_splade (bool): Flag to indicate whether to calculate SPLADE embeddings.

        Returns:
            Tuple[Optional[Dict[str, any]], Optional[Dict[str, any]]]: A tuple containing two dictionaries:
                - The first dictionary maps node IDs to their dense embeddings (if calculated).
                - The second dictionary maps node IDs to their SPLADE embeddings (if calculated).
                Both elements in the tuple will be None if their respective calculations are not performed.

        Raises:
            Exception: Specific exceptions related to model failures or network issues can be raised.

        Usage:
            # Assuming 'node_list' is pre-defined and both embedding flags are true
            dense_emb, splade_emb = await calculate_embeddings(node_list, True, True)
        """
        id_to_dense_embedding, id_to_splade_embedding = None, None
        if do_embedding:
            dense_embeddings = await self.embed_model.aget_text_embedding_batch(
                [node.get_content(metadata_mode="embed") for node in nodes], show_progress=True
                )
            id_to_dense_embedding = {
                node.id_: dense_embedding for node, dense_embedding in zip(nodes, dense_embeddings)
            }

        if do_splade:
            splade_embeddings = await self.sparse_model.aget_text_embedding_batch(
            [node.get_content(metadata_mode="embed") for node in nodes], show_progress=True
            )
            id_to_splade_embedding = {
                node.id_: sparse_embedding for node, sparse_embedding in zip(nodes, splade_embeddings)
            }
        return id_to_dense_embedding, id_to_splade_embedding
    
    async def generate_children_nodes(
            self,
            s3_object: str,
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
        fulltext_content = download_s3_file(self.s3_bucket, s3_object=s3_object)
        file_name = s3_object.split("/")[-1]
        cur_children_dict = defaultdict(list)

        if fulltext_content:
            parsed_fulltext = await self.parse_clean_fulltext(
                fulltext=fulltext_content,
                name=file_name,
                split_depth=self.split_depth)
            for k, values in parsed_fulltext.items():
                base_nodes: List[Document] = [Document(text=each_value) for each_value in values]
                        
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
            id: int,
            abstract: str,
            split_depth: int,
            **kwargs
        ) -> None:
        """
        Asynchronously inserts a single record into the vector datastore.

        Args:
            id (int): A unique identifier for the record.
            abstract (str): A concise summary of the content of the record.
            split_depth (int): The depth at which to split the content of the record.
            **kwargs: Additional keyword arguments that may be used to customize the insertion process.

        Raises:
            asyncio.TimeoutError: If an insertion task does not complete within the expected time.
            ConnectionError: If there is a failure in connecting to the datastore.

        Returns:
            None: This function does not return any value but completes the insertion of the record.

        Example of usage:
            await ainsert_single_record(id=123, abstract="Example abstract", split_depth=2)
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
                s3_object=kwargs.get("fulltext_s3_loc")
            )
        
        #prepare all the parent nodes and children nodes(if any)
        children_nodes =  [item for sublist in cur_children_dict.values() for item in sublist]
        all_cur_nodes = parent_nodes + children_nodes
        logger.info(f"ainsert_single_record. parent_id: {parent_id}. children_nodes_count: {len(children_nodes)}")
        logger.info(f"ainsert_single_record. parent_id: {parent_id}. all_cur_nodes: {len(all_cur_nodes)}")
        
        #call the embedding and splade embedding apis       
        dense_emb, splade_emb = await self.calculate_embeddings(
            nodes=all_cur_nodes,
            do_embedding=True,
            do_splade=True
        )
        logger.info(f"ainsert_single_record. parent_id: {parent_id}. dense_emb length: {len(dense_emb)}")
        logger.info(f"ainsert_single_record. parent_id: {parent_id}. splade_emb length: {len(splade_emb)}")
        
        #prepare the tree here
        cur_level = self.tree_depth - 1
        nodes_to_be_added = []
        while cur_level >= 0:
            
            #set parent node
            if cur_level == 1:
                for p_node in parent_nodes:
                    cur_node = p_node
                    cur_node.metadata["level"] = 1
                    cur_node.metadata["node_type"] = BaseNodeTypeEnum.Parentnode.value
                    nodes_to_be_added.append(cur_node)
            #set children nodes
            elif cur_level == 0:
                if len(children_nodes) > 0:
                    #cluster create 

                    for k, children_section_nodes in cur_children_dict.items():                 
                        children_section_nodes_ids = [node.id for node in children_section_nodes]
                        cur_id_to_dense_embedding = {k: v for k, v in dense_emb.items() if k in children_section_nodes_ids}

                        nodes_per_cluster = get_clusters(
                            children_section_nodes,
                            cur_id_to_dense_embedding
                        )
                    
                        for cluster, summary_doc in zip(nodes_per_cluster, children_section_nodes):
                            current_cluster_id = str(uuid.uuid4())
                            for cur_node in cluster:
                                cur_node.metadata["level"] = 0
                                cur_node.metadata["node_type"] = BaseNodeTypeEnum.ChildNode.value
                                cur_node.metadata["cluster_id"] = current_cluster_id
                                nodes_to_be_added.append(cur_node)
        cur_level = cur_level - 1

        #metadata update for the nodes
        excluded_keys = ["level", "node_type", "cluster_id", "parent_id"]
        for node in nodes_to_be_added:  
            node.metadata["parent_id"] = parent_id 
            node.embedding = dense_emb[node.id_]
            node.splade_embedding = splade_emb[node.id_]
            for key in excluded_keys:
                node.excluded_embed_metadata_keys.append(key)
                node.excluded_llm_metadata_keys.append(key)

        #ready for the insertion to vectorDB
        
    #     self.index.insert_nodes(nodes_to_be_added)

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
                    self.split_depth,
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

        self.splade_doc_embedding_url = self.settings.spladedoc.api_url
        self.splade_doc_embedding_token = self.settings.spladedoc.api_key.get_secret_value()
        self.splade_doc_embedding_batch_size = self.settings.spladedoc.embed_batch_size

        self.qdrant_url_address = self.settings.qdrant.api_url
        self.qdrant_url_port = self.settings.qdrant.api_port
        self.qdrant_collection_name = self.settings.qdrant.collection_name
        self.qdrant_api_key = self.settings.qdrant.api_key.get_secret_value()

        self.fulltext_not_found = []
        self.embed_model = TextEmbeddingsInference(
            model_name="",
            base_url=self.text_embedding_url,
            auth_token=self.text_embedding_token,
            timeout=60,
            embed_batch_size=self.text_embedding_batch_size)
        
        self.sparse_model = SpladeEmbeddingsInference(
            model_name="",
            base_url=self.splade_doc_embedding_url,
            auth_token=self.splade_doc_embedding_token,
            timeout=60,
            embed_batch_size=self.splade_doc_embedding_batch_size)

        self.aclient = AsyncQdrantClient(
            url=self.qdrant_url_address,
            port=self.qdrant_url_port, 
            api_key=self.qdrant_api_key,
            https=False
            )      
        self.vector_store = QdrantVectorStore(client=self.aclient, collection_name=self.qdrant_collection_name)
        self.storage_context = StorageContext.from_defaults(vector_store=self.vector_store)
        self.index = VectorStoreIndex(
            storage_context=self.storage_context
        )   