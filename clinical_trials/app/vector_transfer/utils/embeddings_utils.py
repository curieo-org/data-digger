import asyncio
from typing import List

from llama_index.embeddings.text_embeddings_inference import \
    TextEmbeddingsInference

from app.settings import Settings
from app.vector_transfer.utils.custom_basenode import CurieoBaseNode
from app.vector_transfer.utils.splade_embedding import \
    SpladeEmbeddingsInference


class EmbeddingUtil:
    def __init__(
        self,
        settings: Settings
    ) -> None:
        
        self.settings = settings
        self.embed_model = TextEmbeddingsInference(
            model_name=self.settings.embedding.model_name,
            base_url=self.settings.embedding.api_url,
            auth_token=self.settings.embedding.api_key.get_secret_value(),
            timeout=self.settings.embedding.timeout,
            embed_batch_size=self.settings.embedding.embed_batch_size
        )

        self.splade_model = SpladeEmbeddingsInference(
            model_name=self.settings.spladedoc.model_name,
            base_url=self.settings.spladedoc.api_url,
            auth_token=self.settings.spladedoc.api_key.get_secret_value(),
            timeout=self.settings.spladedoc.timeout,
            embed_batch_size=self.settings.spladedoc.embed_batch_size
        )
        
    async def calculate_dense_embeddings(
        self,
        nodes: List[CurieoBaseNode]
    ):        
        dense_embeddings = await self.embed_model.aget_text_embedding_batch(
            [node.get_content(metadata_mode="embed") for node in nodes]
        )

        id_to_dense_embedding = {
            node.id_: dense_embedding for node, dense_embedding in zip(nodes, dense_embeddings)
        }

        return id_to_dense_embedding

    async def calculate_sparse_embeddings(
        self,
        nodes: List[CurieoBaseNode]
    ):
        sparse_embeddings = self.splade_model.get_text_embedding_batch(
            [node.get_content(metadata_mode="embed") for node in nodes]
        )

        indices = [[entry.get('index') for entry in sublist] for sublist in sparse_embeddings]
        vectors = [[entry.get('value') for entry in sublist] for sublist in sparse_embeddings]
        
        assert len(indices) == len(vectors) == len(nodes)

        id_to_sparse_embedding = {
            node.id_: {"indices": index, "vector": vector} for node, index, vector in zip(nodes, indices, vectors)
        }

        return id_to_sparse_embedding
    
    async def calculate_dense_sparse_embeddings(self, nodes: List[CurieoBaseNode]) -> List[CurieoBaseNode]:
        id_to_dense_embedding, id_to_sparse_embedding = await asyncio.gather(
            self.calculate_dense_embeddings(nodes),
            self.calculate_sparse_embeddings(nodes)
        )
        
        for node in nodes:
            node.embedding, node.sparse_embedding = id_to_dense_embedding[node.id_], id_to_sparse_embedding[node.id_]
        
        return nodes