from dataclasses import Field
from typing import List, Optional
from llama_index.core.schema import (
    BaseNode,
    NodeWithScore,
    TextNode,
    Document
)


class CustomBaseNode(BaseNode):
    """Custom node Object.

    Generic abstract interface for retrievable nodes

    """
    splade_embedding: Optional[List[float]] = Field(
        default=None, description="Embedding of the node."
    )