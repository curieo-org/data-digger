import asyncio
from typing import List
from loguru import logger

from llama_index.core.schema import BaseNode
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference
import llama_index.core.instrumentation as instrument

from utils.splade_embedding import SpladeEmbeddingsInference
from utils.custom_basenode import CurieoBaseNode
from settings import Settings
import numpy as np

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")


class EmbeddingUtil:
    def __init__(
            self,
            settings: Settings) -> None:
        
        self.settings = settings
        self.embed_model = TextEmbeddingsInference(
            model_name="",
            base_url=self.settings.embedding.api_url,
            auth_token=self.settings.embedding.api_key.get_secret_value(),
            timeout=60,
            embed_batch_size=self.settings.embedding.embed_batch_size
            )
        self.splade_model = SpladeEmbeddingsInference(
            model_name="",
            base_url=self.settings.spladedoc.api_url,
            auth_token=self.settings.spladedoc.api_key.get_secret_value(),
            timeout=60,
            embed_batch_size=self.settings.spladedoc.embed_batch_size
            )
        
    async def calculate_dense_embeddings(self, nodes: List[CurieoBaseNode]):
        id_to_dense_embedding = {}
        dense_embeddings = await self.embed_model.aget_text_embedding_batch(
            [node.get_content(metadata_mode="embed") for node in nodes]
            )
        id_to_dense_embedding = {
            node.id_: dense_embedding for node, dense_embedding in zip(nodes, dense_embeddings)
        }
        return id_to_dense_embedding

    async def calculate_sparse_embeddings(self, nodes: List[CurieoBaseNode]):
        id_to_sparse_embedding = {}
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
            self.calculate_dense_embeddings(nodes), self.calculate_sparse_embeddings(nodes)
            )
        for node in nodes:
            node.embedding, node.sparse_embedding = id_to_dense_embedding[node.id_], id_to_sparse_embedding[node.id_]
        
        return nodes


# indices: a list of np.array containing the dimensions of multiple sparse vectors
# values:  a list of np.array containing the non-zero values of multiple sparse vectors
# returns: a tuple of (dimensions, value) representing the average vector
def average_sparse(indices:List, values:List)->List:
    summated = {}
    if len(indices) != len(values):
        raise ValueError("indices must have same length as values")
    if len(indices) == 0:
        return np.array([], dtype=int), np.array([], dtype=float)
    vector_length_sum = 0.0
    dimension_count_sum = 0
    for i in range(0, len(indices)):
        v_index = indices[i]
        v_value = values[i]
        vector_length_squared = 0.0
        for j in range(0, len(v_index)):
            summated[v_index[j]] = summated.get(v_index[j], 0.0) + v_value[j]
            vector_length_squared += v_value[j]*v_value[j]
        dimension_count = len(v_index)
        vector_length_sum = vector_length_sum + vector_length_squared**(1/float(dimension_count))
        dimension_count_sum = dimension_count_sum + dimension_count
    avg_vector_length = vector_length_sum/float(len(indices))
    avg_dimension_count = int(dimension_count_sum/float(len(indices)))
    sum_list = [(k,v) for k,v in summated.items()]
    sum_list.sort(key=lambda x : -x[1])
    sum_list = sum_list[:avg_dimension_count] # chop off the excess
    vector_length = sum(v[1]*v[1] for v in sum_list)**(1/float(len(sum_list))) # current vector length

    indices = np.array([k for (k, v) in sum_list], dtype=int)
    values  = np.array([((v / vector_length) * avg_vector_length) for (_, v) in sum_list], dtype=float)
    
    return (indices, values)
