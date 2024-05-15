from typing import List, Tuple
from loguru import logger

from qdrant_client import QdrantClient
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference
from llama_index.vector_stores.qdrant import QdrantVectorStore
from llama_index.core import (
    StorageContext,
    VectorStoreIndex
)

from settings import Settings
from utils.splade_embedding import SpladeEmbeddingsInference

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class VectorsUpload:

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
    
    def __init__(self,
                settings: Settings):
        """
        Initializes the PubmedDatabaseReader with necessary settings and configurations.
        
        Args:
            settings (Settings): Contains all necessary database configurations.
        """
        self.settings = settings

        self.embed_model = TextEmbeddingsInference(
            model_name="",
            base_url=self.settings.embedding.api_url,
            auth_token=self.settings.embedding.api_key.get_secret_value(),
            timeout=60,
            embed_batch_size=self.settings.embedding.embed_batch_size)
        
        self.splade_model = SpladeEmbeddingsInference(
            model_name="",
            base_url=self.settings.spladedoc.api_url,
            auth_token=self.settings.spladedoc.api_key.get_secret_value(),
            timeout=60,
            embed_batch_size=self.settings.spladedoc.embed_batch_size)

        self.client = QdrantClient(
            url=self.settings.qdrant.api_url, 
            port=self.settings.qdrant.api_port,
            api_key=self.settings.qdrant.api_key.get_secret_value(),
            https=False
            )   

        self.vector_store = QdrantVectorStore(
            client=self.client,
            collection_name=self.settings.qdrant.collection_name, 
            sparse_doc_fn=self.sparse_doc_vectors,
            enable_hybrid=True,
            )
        
        self.storage_context = StorageContext.from_defaults(vector_store=self.vector_store)
        self.index = VectorStoreIndex(
            storage_context=self.storage_context,
            embed_model = self.embed_model,
            nodes =[]
        )   