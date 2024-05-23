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
from utils.database_utils import run_insert_sql
from utils.utils import ProcessingResultEnum, update_result_status
from utils.embeddings_utils import calculate_embeddings

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class ProcessParentNodes:
    def __init__(self,
                settings: Settings):
        """
        Initializes the PubmedDatabaseReader with necessary settings and configurations.
        
        Args:
            settings (Settings): Contains all necessary database configurations.
        """
        self.settings = settings
        self.num_workers = 16
        self.engine = create_engine(self.settings.psql.connection.get_secret_value())
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
    
    async def process_single_parent_record(self, record: dict):
        record_id = int(record.get('identifier'))

        if not self.is_valid_record(record, record_id):
            self.log_bad_data(record_id)
            return

        abstract = self.get_abstract(record)
        if not abstract:
            self.log_bad_data(record_id)
            return

        parent_nodes = self.create_parent_nodes(abstract)
        parent_id = parent_nodes[0].id_

        id_to_dense_embedding = await calculate_embeddings(self.embed_model, parent_nodes)
        self.assign_embeddings_to_nodes(parent_nodes, id_to_dense_embedding)

        nodes_ready_to_be_added = await self.node_metadata_transform(parent_id, record_id, parent_nodes, record)
        self.insert_nodes_into_index(record_id, parent_id, nodes_ready_to_be_added)

    def is_valid_record(self, record: Union[tuple, dict], record_id: int) -> bool:
        abstract_key = self.settings.database_reader.parsed_record_abstract_key
        return record_id > 0 and bool(record.get(abstract_key))

    def log_bad_data(self, record_id: int):
        self.log_dict.append(
            update_result_status(pubmed_id=record_id, status=ProcessingResultEnum.ID_ABSTRACT_BAD_DATA.value)
        )

    def get_abstract(self, record: Union[tuple, dict]) -> str:
        abstract_key = self.settings.database_reader.parsed_record_abstract_key
        return ",".join(abstract["string"] for abstract in record.get(abstract_key, []))

    def create_parent_nodes(self, abstract: str) -> List[Document]:
        return run_transformations(
            nodes=[Document(text=abstract)],
            transformations=[
                SentenceSplitter(
                    chunk_size=self.settings.d2vengine.parent_chunk_size,
                    chunk_overlap=self.settings.d2vengine.parent_chunk_overlap
                )
            ],
            in_place=False
        )

    def assign_embeddings_to_nodes(self, nodes: List[Document], id_to_dense_embedding: dict):
        for node in nodes:
            node.embedding = id_to_dense_embedding[node.id_]

    def insert_nodes_into_index(self, record_id: int, parent_id: str, nodes_to_be_added: List[Document]):
        try:
            self.vu.parent_vectordb_index.insert_nodes(nodes_to_be_added)
            self.log_dict.append(
                update_result_status(
                    pubmed_id=record_id,
                    status=ProcessingResultEnum.SUCCESS.value,
                    parent_id_nodes_count=len(nodes_to_be_added),
                    parent_id=parent_id,
                )
            )
        except Exception as e:
            logger.exception(f"VectorDb problem: {e}")
            self.log_dict.append(
                update_result_status(pubmed_id=record_id, status=ProcessingResultEnum.VECTORDB_FAILED.value)
            )
        
    async def process_batch_parent_records(self,
                                    records: list[defaultdict]) -> None:
        jobs = []
        for each_record in records:           
            jobs.append(
                self.process_single_parent_record(each_record)
            )

        lock = asyncio.Semaphore(self.num_workers)
        # run the jobs while limiting the number of concurrent jobs to num_workers
        for job in jobs:
            async with lock:
                await job

    async def batch_process_records_to_vectors(self,
                                               records: defaultdict, 
                                               batch_size:int = 100):
        keys = list(records.keys())
        total_batches = (len(keys) + batch_size - 1) // batch_size
        for i in tqdm(range(total_batches), desc="Transforming batches"):
            self.log_dict = []
            start_index = i * batch_size
            end_index = start_index + batch_size
            batch_data = [records[key] for key in keys[start_index:end_index]]

            start_time = time.time()
            await self.process_batch_parent_records(batch_data)
            logger.info(f"Processed Batch size of {batch_size} in {time.time() - start_time:.2f}s")
            run_insert_sql(engine=self.engine,
                                 table_name=self.settings.database_reader.pubmed_parent_ingestion_log,
                                 data_dict=self.log_dict)

        logger.info(f"Processed Completed!!!")     
