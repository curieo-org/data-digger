import uuid
from loguru import logger
from functools import reduce
import operator

from llama_index.core.schema import BaseNode
import llama_index.core.instrumentation as instrument

from utils.clustering import get_clusters
from settings import Settings
from utils.embeddings_utils import EmbeddingUtil
from utils.custom_basenode import CurieoBaseNode

import numpy as np

dispatcher = instrument.get_dispatcher(__name__)
logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class TreefyNodes:  
    def __init__(self,
                settings: Settings):
        
        self.settings = settings
        self.eu = EmbeddingUtil(settings)
    
    def tree_children_transformation(
            self,
            record_id: str,
            children_nodes: list[BaseNode]= [],
            cur_children_dict: dict = {}
        ) -> list[CurieoBaseNode]:

        #call the embedding and splade embedding apis       
        children_nodes = self.eu.calculate_dense_sparse_embeddings(children_nodes)
        
        clusters = []
        node_text_details = []
        if len(children_nodes) > 0:
            for children_section_nodes in cur_children_dict.values():                 
                cur_id_to_dense_embedding = {node.node_id: node.get_embedding() for node in children_section_nodes}

                if len(children_section_nodes):
                    nodes_per_cluster = get_clusters(children_section_nodes, cur_id_to_dense_embedding)
                    for cluster in nodes_per_cluster:
                        current_cluster_id = str(uuid.uuid4())
                        metadata = {}
                        metadata["children_node_ids"] = [node.id_ for node in cluster]
                        metadata["pubmedid"] = record_id

                        dense_embeddings = [np.array(node.get_embedding(), dtype=float) for node in cluster]
                        dense_centroid = np.mean(dense_embeddings, axis=0).tolist()

                        sparse_indices_embeddings = [np.array(node.get_sparse_embedding().get('indices'), dtype=int) for node in cluster]
                        sparse_vector_embeddings = [np.array(node.get_sparse_embedding().get('vector'), dtype=float) for node in cluster]

                        sparse_indices_embeddings = [indices for indices in sparse_indices_embeddings if len(indices) != 0]
                        sparse_vector_embeddings = [values for values in sparse_vector_embeddings if len(values) != 0]
                        if len(sparse_vector_embeddings) == 0:
                            # note sure what to do here
                            pass
                        sparse_centroid = self.eu.average_sparse(sparse_indices_embeddings, sparse_vector_embeddings)

                        clusters.append(
                            CurieoBaseNode(
                                id_=current_cluster_id,
                                embedding=dense_centroid,
                                sparse_embedding=sparse_centroid,
                                metadata=metadata
                            )
                        )
                        node_text_details.append([
                            {"id": node.node_id, "node_text": node.get_content(metadata_mode="embed")}
                            for node in cluster
                            ])
        return clusters, reduce(operator.add, node_text_details)
