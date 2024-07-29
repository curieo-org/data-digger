from loguru import logger

from qdrant_client import QdrantClient
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference
from llama_index.vector_stores.qdrant import QdrantVectorStore
from llama_index.core import (
    StorageContext,
    VectorStoreIndex
)

from settings import Settings
from utils.custom_vectorstore import CurieoVectorStore

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class VectorsUpload:
    
    def __init__(self,
                settings: Settings):
        """
        Initializes the VectorsUpload with necessary settings and configurations.
        
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

        self.parent_client = QdrantClient(
            url=self.settings.qdrant.parent_api_url, 
            port=self.settings.qdrant.api_port,
            api_key=self.settings.qdrant.api_key.get_secret_value(),
            https=False
            )  
        self.cluster_client = QdrantClient(
            url=self.settings.qdrant.cluster_api_url, 
            port=self.settings.qdrant.api_port,
            api_key=self.settings.qdrant.api_key.get_secret_value(),
            https=False
            )   

        self.parent_vector_store = CurieoVectorStore(
            client=self.parent_client,
            collection_name=self.settings.qdrant.parent_collection_name
            )
        
        self.cluster_vector_store = CurieoVectorStore(
            client=self.cluster_client,
            collection_name=self.settings.qdrant.cluster_collection_name
            )
        
        self.parent_storage_context = StorageContext.from_defaults(vector_store=self.parent_vector_store)
        self.cluster_storage_context = StorageContext.from_defaults(vector_store=self.cluster_vector_store)
        
        self.parent_vectordb_index = VectorStoreIndex(
            storage_context=self.parent_storage_context,
            embed_model = self.embed_model,
            nodes =[]
        )  
        self.cluster_vectordb_index = VectorStoreIndex(
            storage_context=self.cluster_storage_context,
            embed_model = self.embed_model,
            nodes =[]
        )   