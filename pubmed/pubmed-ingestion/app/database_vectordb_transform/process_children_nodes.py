## WORKING

import asyncio
from collections import defaultdict
from typing import List, Union
from sqlalchemy import create_engine
from loguru import logger
from tqdm import tqdm
from tqdm.asyncio import tqdm
import time

from llama_index.core.schema import (
    BaseNode,
    Document,
    TextNode
)
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.ingestion import run_transformations
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference

from settings import Settings
from database_vectordb_transform.treefy_nodes import TreefyNodes
from database_vectordb_transform.vectors_upload import VectorsUpload
from utils.process_jats import JatsXMLParser
from utils.database_utils import run_insert_sql
from utils.utils import ProcessingResultEnum, update_result_status, download_s3_file, update_result_status, is_valid_record, get_abstract
from utils.embeddings_utils import calculate_embeddings

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class ProcessChildrenNodes:
    def __init__(self,
                settings: Settings):
        """
        Initializes the PubmedDatabaseReader with necessary settings and configurations.
        
        Args:
            settings (Settings): Contains all necessary database configurations.
        """
        self.settings = settings
        self.processed_records = defaultdict(list)
        self.children_pubmed_ids = []
        self.fulltext_pmc_sources = defaultdict(list)
        self.num_workers = 32
        self.engine = create_engine(self.settings.psql.connection.get_secret_value())
        self.tn = TreefyNodes(settings)
        self.embed_model = TextEmbeddingsInference(
            model_name="",
            base_url=self.settings.embedding.api_url,
            auth_token=self.settings.embedding.api_key.get_secret_value(),
            timeout=60,
            embed_batch_size=self.settings.embedding.embed_batch_size)
        self.vu = VectorsUpload(settings)

    async def node_metadata_transform(self, parent_id, pubmedid, nodes_to_be_added, record) -> list[BaseNode]:
        result: list[TextNode] = []
        keys_to_update = ["publicationDate", "year", "authors", "identifiers"]
        for node in nodes_to_be_added:
            for key in keys_to_update:
                node.metadata[key] = record.get(key, "")

        #metadata update for the nodes
        excluded_keys = ["pubmedid"] + keys_to_update
        for node in nodes_to_be_added:
            node.metadata["pubmedid"] = pubmedid
            node.metadata["parent_id"] = parent_id
            
            for key in excluded_keys:
                node.excluded_embed_metadata_keys.append(key)
                node.excluded_llm_metadata_keys.append(key)
            result.append(node)
        return result
    
    async def parse_clean_fulltext(
            self,
            fulltext: str,
            name: str,
            split_depth: int):
        jatParser = JatsXMLParser(name=name, xml_data=fulltext)
        parsed_details = jatParser.parse_root_node()

        to_be_processed_sections = [
            section for section in parsed_details.get('body_sections', [])
            if section.get('title') and section['title'] not in ["Title", "Abstract"]
        ]

        # Process paragraphs
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
    
    async def generate_children_nodes(
            self,
            fulltext_content: str,
            file_name: str
    ):
        cur_children_dict = defaultdict(list)
        parsed_fulltext = await self.parse_clean_fulltext(fulltext=fulltext_content, name=file_name, split_depth=self.settings.jatsparser.split_depth)
            
        #process the retrieved data
        for k, values in parsed_fulltext.items():
            base_nodes: List[Document] = [Document(text=each_value) for each_value in values if each_value.strip()]
            cur_children_dict[k] = run_transformations(base_nodes, transformations=[
                SentenceSplitter(chunk_size=self.settings.d2vengine.child_chunk_size, chunk_overlap=self.settings.d2vengine.child_chunk_overlap)
                ],
                in_place=False)
        return cur_children_dict, parsed_fulltext
    
    async def process_single_child_id(self, record: dict):
        record_id = int(record.get('identifier'))

        if not is_valid_record(record, self.settings.database_reader.parsed_record_abstract_key, record_id):
            self.log_dict.append(
                update_result_status(mode="children", pubmed_id=record_id, status=ProcessingResultEnum.ID_ABSTRACT_BAD_DATA.value)
            )
            return

        abstract = get_abstract(record, self.settings.database_reader.parsed_record_abstract_key)
        if not abstract:
            self.log_dict.append(
                update_result_status(mode="children", pubmed_id=record_id, status=ProcessingResultEnum.ID_ABSTRACT_BAD_DATA.value)
            )
            return
        
        pmc_location = "bulk/" + self.pmc_sources.get(record_id, "") 
        if not pmc_location:
            self.log_dict.append(
                update_result_status(mode="children", pubmed_id=record_id, status=ProcessingResultEnum.PMC_RECORD_NOT_FOUND.value)
            )
            return
        
        fulltext_content = download_s3_file(self.settings.jatsparser.bucket_name, s3_object=pmc_location)

        if fulltext_content:
            cur_children_dict, parsed_fulltext = await self.generate_children_nodes(fulltext_content, pmc_location.split("/")[-1])
            children_nodes = [item for sublist in cur_children_dict.values() for item in sublist]
        else:
            self.log_dict.append(
                update_result_status(mode="children", pubmed_id=id, status=ProcessingResultEnum.PMC_RECORD_PARSING_FAILED.value)
            )
        
        id_to_dense_embedding = await calculate_embeddings(self.embed_model, children_nodes)
        r = await self.tn.tree_children_transformation(id_to_dense_embedding, cur_children_dict)

        
        self.assign_embeddings_to_nodes(nodes_ready_to_b_added, id_to_dense_embedding)
            

        #all nodes operation Marius will add the details here


        self.insert_nodes_into_index()

    def assign_embeddings_to_nodes(self, nodes: List[Document], id_to_dense_embedding: dict):
        for node in nodes:
            node.embedding = id_to_dense_embedding[node.id_]

    def insert_nodes_into_index(self, record_id: int, parent_id: str, nodes_to_be_added: List[Document]):
        try:
            self.vu.cluster_storage_context.insert_nodes(nodes_to_be_added)
        except Exception as e:
            logger.exception(f"VectorDb problem: {e}")
            self.log_dict.append(
                update_result_status(pubmed_id=record_id, status=ProcessingResultEnum.VECTORDB_FAILED.value)
            )
        
    async def process_batch_children_ids(self,
                                    records: list[defaultdict]) -> None:
        jobs = []
        for each_record in records:           
            jobs.append(
                self.process_single_child_id(each_record)
            )

        lock = asyncio.Semaphore(self.num_workers)
        # run the jobs while limiting the number of concurrent jobs to num_workers
        for job in jobs:
            async with lock:
                await job

    async def batch_process_children_ids_to_vectors(self,
                                               records: defaultdict, 
                                               pmc_sources: defaultdict,
                                               batch_size:int = 100):
        keys = list(records.keys())
        self.pmc_sources = pmc_sources
        total_batches = (len(keys) + batch_size - 1) // batch_size
        for i in tqdm(range(total_batches), desc="Transforming batches"):
            self.log_dict = []
            start_index = i * batch_size
            end_index = start_index + batch_size
            batch_data = [records[key] for key in keys[start_index:end_index]]

            start_time = time.time()
            await self.process_batch_children_ids(batch_data)
            logger.info(f"Processed Batch size of {batch_size} in {time.time() - start_time:.2f}s")
            run_insert_sql(engine=self.engine,
                                 table_name=self.settings.database_reader.pubmed_children_ingestion_log,
                                 data_dict=self.log_dict)

        logger.info(f"Processed Completed!!!")  
