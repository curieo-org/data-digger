import asyncio
import time
from collections import defaultdict
from typing import List

from llama_index.core.ingestion import run_transformations
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import TextNode
from llama_index.embeddings.text_embeddings_inference import \
    TextEmbeddingsInference
from loguru import logger
from tqdm import tqdm
from tqdm.asyncio import tqdm

from app.database_transfer import PGEngine, TableStructure
from app.settings import Settings
from app.vector_transfer.database_reader import DatabaseReader
from app.vector_transfer.utils import CurieoBaseNode, EmbeddingUtil
from app.vector_transfer.vectors_upload import VectorsUpload

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class VectorTransferProcessor:
    def __init__(
        self,
        database_engine: PGEngine,
        settings: Settings
    ):
        """
        Initializes the PubmedDatabaseReader with necessary settings and configurations.
        
        Args:
            settings (Settings): Contains all necessary database configurations.
        """
        self.settings = settings
        self.num_workers = settings.d2vengine.max_workers
        self.batch_size = settings.d2vengine.batch_size
        self.database_reader = DatabaseReader(
            database_engine=database_engine,
        )
        self.embed_model = TextEmbeddingsInference(
            model_name=self.settings.embedding.model_name,
            base_url=self.settings.embedding.api_url,
            auth_token=self.settings.embedding.api_key.get_secret_value(),
            timeout=self.settings.embedding.timeout,
            embed_batch_size=self.settings.embedding.embed_batch_size
        )
        self.vu = VectorsUpload(settings)
        self.eu = EmbeddingUtil(settings)

    async def node_metadata_transform(
        self,
        table_structure: TableStructure,
        nodes_to_be_added: list[CurieoBaseNode],
        row: dict,
    ) -> list[CurieoBaseNode]:
        columns_to_be_added = table_structure.vector_metadata_columns + table_structure.primary_keys

        for node in nodes_to_be_added:
            for key in columns_to_be_added:
                node.metadata[key] = row[key]

        # metadata update for the nodes
        result: list[CurieoBaseNode] = []

        for node in nodes_to_be_added:            
            for key in columns_to_be_added:
                node.excluded_embed_metadata_keys.append(key)
                node.excluded_llm_metadata_keys.append(key)

            result.append(node)

        return result
    
    async def process_single_row(
        self,
        table_structure: TableStructure,
        row: dict
    ) -> None:
        text_value = " ".join([str(row[column]) for column in table_structure.embeddable_columns])

        nodes = [CurieoBaseNode.from_text_node(text_node) for text_node in self.create_nodes(text_value)]
        nodes_with_embeddings = await self.eu.calculate_dense_sparse_embeddings(nodes)

        nodes_ready_to_be_added = await self.node_metadata_transform(
            table_structure,
            nodes_with_embeddings,
            row,
        )
        self.insert_nodes_into_index(nodes_ready_to_be_added)

    def create_nodes(self, text_value: str) -> List[TextNode]:
        return run_transformations(
            nodes=[TextNode(text=text_value)],

            transformations=[
                SentenceSplitter(
                    chunk_size=self.settings.d2vengine.chunk_size,
                    chunk_overlap=self.settings.d2vengine.chunk_overlap
                )
            ],

            in_place=False
        )
        
    def insert_nodes_into_index(
        self,
        nodes_to_be_added: List[CurieoBaseNode]
    ) -> None:
        try:
            self.vu.vectordb_index.insert_nodes(nodes_to_be_added)
        except Exception as e:
            logger.exception(f"VectorDb problem: {e}")
        
    async def process_batch_rows(
        self,
        table_structure: TableStructure,
        rows: list[defaultdict]
    ) -> None:
        jobs = []
        for each_row in rows:           
            jobs.append(
                self.process_single_row(
                    table_structure=table_structure,
                    row=each_row
                )
            )

        lock = asyncio.Semaphore(self.num_workers)
        # run the jobs while limiting the number of concurrent jobs to num_workers
        for job in jobs:
            async with lock:
                await job

    async def database_to_vectors(
        self,
        table_structure: TableStructure
    ) -> None:
        for rows in tqdm(
            self.database_reader.load_data_in_batches(
                table_structure,
                self.batch_size
            ),
            desc="Transforming batches"
        ):
            start_time = time.time()

            await self.process_batch_rows(
                table_structure=table_structure,
                rows=rows
            )

            logger.info(f"Processed Batch size of {self.batch_size} in {time.time() - start_time:.2f}s")

        logger.info("Processed Completed!!!")     