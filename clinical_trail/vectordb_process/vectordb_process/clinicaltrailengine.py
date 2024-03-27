from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from functools import partial

from llama_index.core.node_parser import TokenTextSplitter, SimpleNodeParser
from llama_index.core.extractors import SummaryExtractor, QuestionsAnsweredExtractor
from llama_index.vector_stores.qdrant import QdrantVectorStore
from llama_index.core import StorageContext, ServiceContext
from llama_index.core.schema import Document, MetadataMode
from llama_index.readers.database import DatabaseReader
from llama_index.core import VectorStoreIndex
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference

from sqlalchemy import create_engine, text
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.models import VectorParams, Distance
from typing import Any, List, Tuple


class ClinicalTrailsDatabaseReader(DatabaseReader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.offset = 0  # Initialize offset

    def load_data_in_batches(self, query_template: str, batch_size: int = 100) -> List[Document]: # type: ignore
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
            query = query_template.format(batch_size=batch_size, offset=offset)

            documents = []
            with self.sql_database.engine.connect() as connection:
                if query is None:
                    raise ValueError("A query parameter is necessary to filter the data")
                else:
                    result = connection.execute(text(query))

                for item in result.fetchall():
                    # Custom processing of each item
                    # Example: Append a suffix to each column value
                    id = str(item[0])
                    row = " ".join(s if s is not None else "" for s in [item[1], item[2]])
                    documents.append(Document(text=row, metadata={"nct_id" : id}))
            yield documents
            offset += batch_size

        
class ClinicalTrailVectorDbEngine:
    def __init__(
            self,
            qdrant_url_address:str,
            qdrant_url_port:int,
            qdrant_collection_name:str,
            qdrant_api_key:str
            ):
        self.client = QdrantClient(
            url=qdrant_url_address,
            port=qdrant_url_port, 
            api_key=qdrant_api_key,
            https=False
            )
        
        self.client.recreate_collection(
            collection_name=qdrant_collection_name,
            vectors_config=VectorParams(size=1024, distance=Distance.COSINE)
            )
        
        self.vector_store = QdrantVectorStore(
            client=self.client,
            collection_name=qdrant_collection_name
            )

        self.node_parser = SimpleNodeParser.from_defaults(chunk_size=1024, chunk_overlap=32)
        self.service_context = ServiceContext.from_defaults(
            embed_model=TextEmbeddingsInference(model_name="", timeout=60, embed_batch_size=4),
            llm=None,
            node_parser=self.node_parser
        )
        self.storage_context = StorageContext.from_defaults(vector_store=self.vector_store)

        self.query_template = """
        SELECT nct_id, title, description FROM public.tbl_studies_info
        LIMIT {batch_size} OFFSET {offset}
        """

        self.batch_size = 100
        self.chunk_size = 10


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
        num_workers = 10
    
        # Split records into chunks
        chunks = list(self.chunkify(records, chunk_size))
        # Setup a process pool and process chunks in parallel

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Executor.map expects a function and iterables for each of the function's arguments
            process_func = partial(self.process_records_chunk, service_context=self.service_context, storage_context=self.storage_context)
            
            list(tqdm(executor.map(process_func, chunks), total=len(chunks)))

    def database_to_vectors(self, database_engine: str):
        reader = ClinicalTrailsDatabaseReader(engine=create_engine(database_engine))
        count = 0
        for batch in tqdm(reader.load_data_in_batches(query_template=self.query_template, batch_size=self.batch_size)):
            self.process_records(list(batch), chunk_size=self.chunk_size)
            count += len(batch)
            print("Processed count  = %d" % count)