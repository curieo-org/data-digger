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
from typing import Any, List, Tuple
import torch
from transformers import AutoTokenizer, AutoModelForMaskedLM

doc_splade_model = "naver/efficient-splade-VI-BT-large-doc"
query_splade_model = "naver/efficient-splade-VI-BT-large-query"
doc_tokenizer = AutoTokenizer.from_pretrained(doc_splade_model)
doc_model = AutoModelForMaskedLM.from_pretrained(doc_splade_model)

query_tokenizer = AutoTokenizer.from_pretrained(query_splade_model)
query_model = AutoModelForMaskedLM.from_pretrained(query_splade_model)


class PubmedDatabaseReader(DatabaseReader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.offset = 0  # Initialize offset

    def load_data_in_batches(self, query_template: str, batch_size: int = 100) -> List[Document]:
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
                    pubid = item[0]
                    doc_str = ",".join([abstract['string'] for abstract in item[1]])
                    title = ",".join([title['string'] for title in item[2]])
                    pubdate = item[3]
                    year = item[4]
                    
                    documents.append(Document(text=doc_str, metadata={"title" : title, "pubid" : pubid, "year": year, "pubdate": pubdate}))
            yield documents
            offset += batch_size

        
class DatabaseVectorsEngine:

    def sparse_doc_vectors(
            self,
            texts: List[str],
        ) -> Tuple[List[List[int]], List[List[float]]]:
        """
        Computes vectors from logits and attention mask using ReLU, log, and max operations.
        """
        tokens = doc_tokenizer(
            texts, truncation=True, padding=True, return_tensors="pt"
        )
        if torch.cuda.is_available():
            tokens = tokens.to("cuda")

        output = doc_model(**tokens)
        logits, attention_mask = output.logits, tokens.attention_mask
        relu_log = torch.log(1 + torch.relu(logits))
        weighted_log = relu_log * attention_mask.unsqueeze(-1)
        tvecs, _ = torch.max(weighted_log, dim=1)

        # extract the vectors that are non-zero and their indices
        indices = []
        vecs = []
        for batch in tvecs:
            indices.append(batch.nonzero(as_tuple=True)[0].tolist())
            vecs.append(batch[indices[-1]].tolist())

        return indices, vecs
    
    def sparse_query_vectors(
            self,
            texts: List[str],
        ) -> Tuple[List[List[int]], List[List[float]]]:
        """
        Computes vectors from logits and attention mask using ReLU, log, and max operations.
        """
        # TODO: compute sparse vectors in batches if max length is exceeded
        tokens = query_tokenizer(
            texts, truncation=True, padding=True, return_tensors="pt"
        )
        if torch.cuda.is_available():
            tokens = tokens.to("cuda")

        output = query_model(**tokens)
        logits, attention_mask = output.logits, tokens.attention_mask
        relu_log = torch.log(1 + torch.relu(logits))
        weighted_log = relu_log * attention_mask.unsqueeze(-1)
        tvecs, _ = torch.max(weighted_log, dim=1)

        # extract the vectors that are non-zero and their indices
        indices = []
        vecs = []
        for batch in tvecs:
            indices.append(batch.nonzero(as_tuple=True)[0].tolist())
            vecs.append(batch[indices[-1]].tolist())

        return indices, vecs

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
            vectors_config={
                "text-dense": models.VectorParams(
                    size=1024,
                    distance=models.Distance.COSINE,
                )
            },
            sparse_vectors_config={
                "text-sparse": models.SparseVectorParams(
                    index=models.SparseIndexParams(
                        on_disk=False,
                        )
                    )
                }
            )
        
        self.vector_store = QdrantVectorStore(
            client=self.client,
            collection_name=qdrant_collection_name,
            enable_hybrid=True,
            sparse_doc_fn=self.sparse_doc_vectors,
            sparse_query_fn=self.sparse_query_vectors,
            )

        self.node_parser = SimpleNodeParser.from_defaults(chunk_size=1024, chunk_overlap=32)
        self.service_context = ServiceContext.from_defaults(
            embed_model=TextEmbeddingsInference(model_name="", timeout=60, embed_batch_size=1),
            llm=None,
            node_parser=self.node_parser
        )
        self.storage_context = StorageContext.from_defaults(vector_store=self.vector_store)

        self.query_template = """
        SELECT identifier, cast(record AS json) -> 'abstractText' as abstract,
        cast(record AS json) -> 'titles' as title,
        cast(record AS json) -> 'publicationDate' as publicationDate,
        year
        FROM public.working_table 
        LIMIT {batch_size} OFFSET {offset}
        """

        self.batch_size = 100
        self.chunk_size = 100


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
        reader = PubmedDatabaseReader(engine=create_engine(database_engine))
        count = 0
        for batch in tqdm(reader.load_data_in_batches(query_template=self.query_template, batch_size=self.batch_size)):
            self.process_records(list(batch), chunk_size=self.chunk_size)
            count += len(batch)
            print("Processed count  = %d" % count)