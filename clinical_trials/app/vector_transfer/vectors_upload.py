from llama_index.core import StorageContext, VectorStoreIndex
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference
from qdrant_client import QdrantClient

from app.settings import Settings
from app.vector_transfer.utils import CurieoVectorStore, SpladeEmbeddingsInference


class VectorsUpload:

    def __init__(self, settings: Settings):
        """
        Initializes the VectorsUpload with necessary settings and configurations.

        Args:
            settings (Settings): Contains all necessary database configurations.
        """
        self.settings = settings

        self.embed_model = TextEmbeddingsInference(
            model_name=self.settings.embedding.model_name,
            base_url=self.settings.embedding.api_url,
            auth_token=self.settings.embedding.api_key.get_secret_value(),
            timeout=self.settings.embedding.timeout,
            embed_batch_size=self.settings.embedding.embed_batch_size,
        )

        self.splade_model = SpladeEmbeddingsInference(
            model_name=self.settings.spladedoc.model_name,
            base_url=self.settings.spladedoc.api_url,
            auth_token=self.settings.spladedoc.api_key.get_secret_value(),
            timeout=self.settings.spladedoc.timeout,
            embed_batch_size=self.settings.spladedoc.embed_batch_size,
        )

        self.client = QdrantClient(
            url=self.settings.qdrant.api_url,
            grpc_port=self.settings.qdrant.api_port,
            prefer_grpc=self.settings.qdrant.prefer_grpc,
            api_key=self.settings.qdrant.api_key.get_secret_value(),
            https=False,
        )

        self.vector_store = CurieoVectorStore(
            client=self.client,
            collection_name=self.settings.qdrant.collection_name,
        )

        self.storage_context = StorageContext.from_defaults(
            vector_store=self.vector_store
        )

        self.vectordb_index = VectorStoreIndex(
            storage_context=self.storage_context, embed_model=self.embed_model, nodes=[]
        )
