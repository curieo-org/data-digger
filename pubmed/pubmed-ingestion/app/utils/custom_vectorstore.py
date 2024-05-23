from typing import Any, List, Tuple
from llama_index.vector_stores.qdrant import QdrantVectorStore

from llama_index.core.schema import BaseNode
from llama_index.core.utils import iter_batch
from llama_index.core.vector_stores.utils import node_to_metadata_dict
from qdrant_client.http import models as rest

from utils.custom_basenode import CurieoBaseNode


class CurieoVectorStore(QdrantVectorStore):

    def _build_points(self, nodes: List[BaseNode]) -> Tuple[List[Any], List[str]]:
        ids = []
        points = []
        for node_batch in iter_batch(nodes, self.batch_size):
            node_ids = []
            vectors: List[Any] = []
            payloads = []

            for i, node in enumerate(node_batch):
                assert isinstance(node, CurieoBaseNode)
                node_ids.append(node.node_id)

                vectors.append(
                    {
                        "text-sparse": rest.SparseVector(
                            indices=node.get_sparse_embedding().get('indices', []),
                            values=node.get_sparse_embedding().get('vector', []),
                        ),
                         "text-dense": node.get_embedding(),
                    }
                )

                metadata = node_to_metadata_dict(
                    node, remove_text=False, flat_metadata=self.flat_metadata
                )
                payloads.append(metadata)

            points.extend(
                [
                    rest.PointStruct(id=node_id, payload=payload, vector=vector)
                    for node_id, payload, vector in zip(node_ids, payloads, vectors)
                ]
            )

            ids.extend(node_ids)

        return points, ids