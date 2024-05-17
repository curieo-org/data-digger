from typing import List
from loguru import logger

from llama_index.core.schema import BaseNode
import llama_index.core.instrumentation as instrument

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")


async def calculate_embeddings(embed_model, nodes: List[BaseNode]):
    id_to_dense_embedding = {}
    dense_embeddings = await embed_model.aget_text_embedding_batch(
        [node.get_content(metadata_mode="embed") for node in nodes]
        )
    id_to_dense_embedding = {
        node.id_: dense_embedding for node, dense_embedding in zip(nodes, dense_embeddings)
    }
    return id_to_dense_embedding