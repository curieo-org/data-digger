import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union
from sqlalchemy import create_engine
from loguru import logger
from tqdm import tqdm
from tqdm.asyncio import tqdm
import time

from llama_index.core.schema import Document
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.ingestion import run_transformations

from settings import Settings
from database_vectordb_transform.treefy_nodes import TreefyNodes
from database_vectordb_transform.vectors_upload import VectorsUpload
from utils.process_jats import JatsXMLParser
from utils.clustering import NodeCluster
from utils.database_utils import (
    run_insert_sql
)
from utils.custom_basenode import CurieoBaseNode
from utils.utils import (
    ProcessingResultEnum, 
    update_result_status, 
    download_s3_file, 
    update_result_status, 
    is_valid_record, 
    get_abstract,
    upload_to_s3
)

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class ProcessChildrenNodes:
    def __init__(self,
                settings: Settings):
        self.settings = settings
        self.processed_records = defaultdict(list)
        self.children_pubmed_ids = []
        self.fulltext_pmc_sources = defaultdict(list)
        self.num_workers = 32
        self.engine = create_engine(self.settings.psql.connection.get_secret_value())
        self.tn = TreefyNodes(settings)
        self.vu = VectorsUpload(settings)
    
    def parse_clean_fulltext(
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

        for key in grouped_ids:
            if isinstance(grouped_ids[key], list):  # Check if the value is a list
                grouped_ids[key] = "".join(grouped_ids[key])

        return grouped_ids
    
    def generate_children_nodes(
            self,
            fulltext_content: str,
            file_name: str
    ):
        cur_children_dict = defaultdict(list)
        parsed_fulltext = self.parse_clean_fulltext(fulltext=fulltext_content, name=file_name, split_depth=self.settings.jatsparser.split_depth)
            
        #process the retrieved data
        for k, text in parsed_fulltext.items():
            if len(text):
                base_nodes: List[Document] = [Document(text=text.strip())]
                transformed_nodes: List[Document] = run_transformations(base_nodes, transformations=[
                    SentenceSplitter(chunk_size=self.settings.d2vengine.child_chunk_size, chunk_overlap=self.settings.d2vengine.child_chunk_overlap)
                    ],
                    in_place=False)
                cur_children_dict[k] = [CurieoBaseNode.from_text_node(node, node_id_generate=True) for node in transformed_nodes]
        return cur_children_dict, parsed_fulltext
    
    def process_single_child_id(self, record: dict):
        record_id = int(record.get('identifier'))

        if not is_valid_record(record, self.settings.database_reader.parsed_record_abstract_key, record_id):
            self.log_dict.append(
                update_result_status(mode="children", pubmed_id=record_id, status=ProcessingResultEnum.ID_ABSTRACT_BAD_DATA.value)
            )
            return False

        abstract = get_abstract(record, self.settings.database_reader.parsed_record_abstract_key)
        if not abstract:
            self.log_dict.append(
                update_result_status(mode="children", pubmed_id=record_id, status=ProcessingResultEnum.ID_ABSTRACT_BAD_DATA.value)
            )
            return False
        
        pmc_location = "bulk/" + self.pmc_sources.get(record_id, "") 
        if not pmc_location:
            self.log_dict.append(
                update_result_status(mode="children", pubmed_id=record_id, status=ProcessingResultEnum.PMC_RECORD_NOT_FOUND.value)
            )
            return False
        
        fulltext_content = download_s3_file(self.settings.jatsparser.bucket_name, s3_object=pmc_location)

        if fulltext_content:
            cur_children_dict, parsed_fulltext = self.generate_children_nodes(fulltext_content, pmc_location.split("/")[-1])
            children_nodes = [item for sublist in cur_children_dict.values() for item in sublist]
            self.fulltext_details[record_id] = parsed_fulltext
        else:
            self.log_dict.append(
                update_result_status(mode="children", pubmed_id=record_id, status=ProcessingResultEnum.PMC_RECORD_PARSING_FAILED.value)
            )
            return False
        
        if len(children_nodes) == 0 or len(cur_children_dict) == 0:
            self.log_dict.append(
                update_result_status(mode="children", pubmed_id=record_id, status=ProcessingResultEnum.NO_PMC_RECORDS.value)
            )
            return False 
        try:
            clusters, node_text_dict = self.tn.tree_children_transformation(record_id, children_nodes, cur_children_dict)
            self.insert_nodes_into_index(record_id, clusters)
            run_insert_sql(self.engine, self.settings.database_reader.pubmed_children_text_details, node_text_dict)
            return True
        except Exception as e:
            self.log_dict.append(
                update_result_status(mode="children", pubmed_id=record_id, status=ProcessingResultEnum.SPARSE_EMBEDDING_CALCULATION_FAILED.value)
            )
            return False


    def assign_embeddings_to_nodes(self, nodes: List[Document], id_to_dense_embedding: dict):
        for node in nodes:
            node.embedding = id_to_dense_embedding[node.id_]

    def insert_nodes_into_index(self, id:int, clusters: List[NodeCluster]):
        try:
            self.vu.cluster_vectordb_index.insert_nodes(clusters)
            self.log_dict.append(
                update_result_status(
                    mode="children",
                    pubmed_id=id,
                    status=ProcessingResultEnum.SUCCESS.value,
                    nodes_count=len(clusters)
                )
            )
        except Exception as e:
            logger.exception(f"VectorDb problem: {e}")
            self.log_dict.append(update_result_status(mode="children", pubmed_id=id, status=ProcessingResultEnum.PMC_RECORD_PARSING_FAILED.value))
     
    async def process_batch_children_ids(self,
                                    records: list[defaultdict]) -> None:
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            jobs = []
            for each_record in records:
                job = loop.run_in_executor(executor, self.process_single_child_id, each_record)
                jobs.append(job)

            lock = asyncio.Semaphore(self.num_workers)
            async with lock:
                results = await asyncio.gather(*jobs)

    def batch_process_children_ids_to_vectors(self,
                                               records: defaultdict, 
                                               pmc_sources: defaultdict,
                                               batch_size:int = 250):
        keys = list(records.keys())
        self.pmc_sources = pmc_sources
        total_batches = (len(keys) + batch_size - 1) // batch_size
        for i in tqdm(range(total_batches), desc="Transforming batches"):
            self.log_dict = []
            self.fulltext_details = {}
            start_index = i * batch_size
            end_index = start_index + batch_size
            batch_data = [records[key] for key in keys[start_index:end_index]]

            start_time = time.time()
            asyncio.run(self.process_batch_children_ids(batch_data))
            logger.info(f"Processed Batch size of {batch_size} in {time.time() - start_time:.2f}s")
            run_insert_sql(engine=self.engine,
                                 table_name=self.settings.database_reader.pubmed_children_ingestion_log,
                                 data_dict=self.log_dict)
            upload_to_s3(self.settings.d2vengine.s3_fulltext_bucket, self.fulltext_details)

        logger.info(f"Processed Completed!!!")  
