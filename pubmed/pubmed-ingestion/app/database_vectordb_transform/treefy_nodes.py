from collections import defaultdict
from typing import List
import uuid
from loguru import logger

from llama_index.core.schema import BaseNode
import llama_index.core.instrumentation as instrument

from utils.clustering import get_clusters, NodeCluster
from settings import Settings
from utils.utils import BaseNodeTypeEnum
from utils.embeddings_utils import EmbeddingUtil

import numpy as np

dispatcher = instrument.get_dispatcher(__name__)
logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class TreefyNodes:  
    def __init__(self,
                settings: Settings):
        
        self.settings = settings
        self.eu = EmbeddingUtil(settings)
    
    async def tree_children_transformation(
            self,
            children_nodes: list[BaseNode]= [],
            cur_children_dict: dict = {}
        ) -> list[NodeCluster]:

        #call the embedding and splade embedding apis       
        dense_emb, sparse_emb = await self.eu.calculate_dense_sparse_embeddings(children_nodes)
        
        clusters = []
        if len(children_nodes) > 0:
            for children_section_nodes in cur_children_dict.values():                 
                children_section_nodes_ids = [node.id_ for node in children_section_nodes]
                cur_id_to_dense_embedding = {k: v for k, v in dense_emb.items() if k in children_section_nodes_ids}

                if len(children_section_nodes):
                    nodes_per_cluster = get_clusters(children_section_nodes, cur_id_to_dense_embedding)
                    for cluster in nodes_per_cluster:
                        current_cluster_id = str(uuid.uuid4())
                        dense_embeddings = [np.array(dense_emb[c.id_], dtype=float) for c in cluster]
                        #sparse_embeddings = [np.array(sparse_emb[c.id_], dtype=float) for c in cluster]
                        dense_centroid = np.mean(dense_embeddings, axis=0)
                        #sparse_centroid = np.mean(sparse_embeddings, axis=0)
                        clusters.append(
                            NodeCluster(current_cluster_id, 
                                        child_ids=[c.id_ for c in cluster], 
                                        dense_centroid=dense_centroid, 
                                        sparse_centroid=None))

        return clusters