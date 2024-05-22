from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import List

from llama_index.core import ServiceContext, StorageContext, VectorStoreIndex
from llama_index.core.node_parser import SimpleNodeParser
from llama_index.core.schema import Document
from llama_index.embeddings.text_embeddings_inference import \
    TextEmbeddingsInference
from llama_index.vector_stores.qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams
from tqdm import tqdm

from app.database_transfer import TableStructure
from app.database_transfer.utils import PGEngine
from app.settings import CTDatabaseReaderSettings, Settings


class ClinicalTrailsDatabaseReader():
    def __init__(
        self,
        database_engine: PGEngine,
        table_structure: TableStructure,
        database_reader: CTDatabaseReaderSettings
    ):
        self.batch_size = database_reader.batch_size
        self.table_structure = table_structure
        self.database_engine = database_engine

        self.query_template = f"""
            SELECT {', '.join(table_structure.embeddable_columns)} FROM {table_structure.table_name}
            LIMIT {self.batch_size} OFFSET {{offset}};
        """

    def load_data_in_batches(self) -> List[Document]: # type: ignore
        """Custom query and load data method.
        
        This overridden version might perform additional operations,
        such as filtering results based on custom criteria or enriching
        the documents with additional information.
        
        Args:
            query (str): Query parameter to filter tables and rows.
        
        Returns:
            List[Document]: A list of custom Document objects.
        """
        offset = 0

        while True:
            query = self.query_template.format(offset=offset)
            if query is None:
                raise ValueError("A query parameter is necessary to filter the data")

            documents = []
            result = self.database_engine.execute_query(query)

            for item in result:
                row_object = dict(zip(self.table_structure.embeddable_columns, item))

                row = " ".join(s if s is not None else "" for s in item)

                documents.append(Document(text=row, metadata=row_object))

            yield documents
            offset += self.batch_size
        
class VectorDBEngine:
    def __init__(
        self,
        settings: Settings,
    ):
        self.settings = settings
        self.chunk_size = settings.embedding.chunk_size
        self.max_workers = settings.embedding.max_workers

        self.client = QdrantClient(
            url=settings.qdrant.api_url,
            port=settings.qdrant.api_port,
            api_key=settings.qdrant.api_key.get_secret_value(),
            https=settings.qdrant.https
        )
        
        self.client.recreate_collection(
            collection_name=settings.qdrant.collection_name,
            vectors_config=VectorParams(
                size=1024,
                distance=Distance.COSINE
            )
        )
        
        self.vector_store = QdrantVectorStore(
            client=self.client,
            collection_name=settings.qdrant.collection_name
        )

        self.node_parser = SimpleNodeParser.from_defaults(
            chunk_size=1024,
            chunk_overlap=32
        )

        self.service_context = ServiceContext.from_defaults(
            embed_model=TextEmbeddingsInference(
                base_url=settings.embedding.api_url,
                auth_token=settings.embedding.api_key.get_secret_value(),
                model_name=settings.embedding.model_name,
                timeout=settings.embedding.timeout,
                embed_batch_size=settings.embedding.embed_batch_size,
            ),
            llm=None,
            node_parser=self.node_parser
        )
        self.storage_context = StorageContext.from_defaults(
            vector_store=self.vector_store
        )


    @staticmethod
    def process_records_chunk(chunk, service_context, storage_context):
        # Process the whole chunk of records at once
        VectorStoreIndex.from_documents(
            documents=chunk,
            show_progress=True,
            service_context=service_context,
            storage_context=storage_context,
        )

    @staticmethod
    def chunkify(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def process_records(self, records: List[Document], chunk_size=1000) -> None:    
        # Split records into chunks
        chunks = list(self.chunkify(records, chunk_size))
        # Setup a process pool and process chunks in parallel

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Executor.map expects a function and iterables for each of the function's arguments
            process_func = partial(
                self.process_records_chunk,
                service_context=self.service_context, 
                storage_context=self.storage_context
            )
            
            list(tqdm(
                executor.map(process_func, chunks),
                total=len(chunks)
            ))

    def database_to_vectors(self, database_engine: PGEngine, table_structure: TableStructure) -> None:
        return
        reader = ClinicalTrailsDatabaseReader(
            database_engine=database_engine,
            table_structure=table_structure,
            database_reader=self.settings.database_reader
        )
        
        count = 0
        for batch in tqdm(reader.load_data_in_batches()):
            self.process_records(
                list(batch),
                chunk_size=self.chunk_size
            )

            count += len(batch)
            print("Processed count  = %d" % count)