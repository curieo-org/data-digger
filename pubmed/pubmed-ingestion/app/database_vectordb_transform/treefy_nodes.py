from collections import defaultdict
from typing import List
import uuid
from loguru import logger

from llama_index.core.schema import BaseNode
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference
import llama_index.core.instrumentation as instrument

from utils.clustering import get_clusters
from settings import Settings
from utils.utils import BaseNodeTypeEnum

dispatcher = instrument.get_dispatcher(__name__)
logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class TreefyNodes:  
    async def calculate_embeddings(
            self,
            nodes: List[BaseNode]):
        id_to_dense_embedding = {}
        dense_embeddings = await self.embed_model.aget_text_embedding_batch(
            [node.get_content(metadata_mode="embed") for node in nodes]
            )
        id_to_dense_embedding = {
            node.id_: dense_embedding for node, dense_embedding in zip(nodes, dense_embeddings)
        }
        return id_to_dense_embedding
    
    async def tree_transformation(
            self,
            parent_nodes: list[BaseNode] = [],
            children_nodes: list[BaseNode]= [],
            cur_children_dict: defaultdict = {}
        ) -> list[BaseNode]:
        #prepare all the parent nodes and children nodes(if any)
        all_cur_nodes = parent_nodes + children_nodes
        
        #call the embedding and splade embedding apis       
        dense_emb = await self.calculate_embeddings(nodes=all_cur_nodes)
        
        #prepare the tree here
        cur_level = self.settings.d2vengine.tree_depth - 1
        nodes_to_be_added = []
        while cur_level >= 0:
            #set parent node
            if cur_level == 1:
                for p_node in parent_nodes:
                    cur_node = p_node
                    cur_node.metadata["level"] = 1
                    cur_node.metadata["node_level"] = BaseNodeTypeEnum.PARENT.value
                    cur_node.embedding = dense_emb[cur_node.id_]
                    nodes_to_be_added.append(cur_node)
            #set children nodes - if any
            elif cur_level == 0 and len(children_nodes) > 0:
                #cluster create 
                for children_section_nodes in cur_children_dict.values():                 
                    children_section_nodes_ids = [node.id_ for node in children_section_nodes]
                    cur_id_to_dense_embedding = {k: v for k, v in dense_emb.items() if k in children_section_nodes_ids}

                    if len(children_section_nodes):
                        nodes_per_cluster = get_clusters(
                            children_section_nodes,
                            cur_id_to_dense_embedding
                        )
                        for cluster, summary_doc in zip(nodes_per_cluster, children_section_nodes):
                            current_cluster_id = str(uuid.uuid4())
                            for cur_node in cluster:
                                cur_node.metadata["level"] = 0
                                cur_node.metadata["node_level"] = BaseNodeTypeEnum.CHILD.value
                                cur_node.metadata["cluster_id"] = current_cluster_id
                                cur_node.embedding = dense_emb[cur_node.id_]
                                nodes_to_be_added.append(cur_node)
            
            cur_level = cur_level - 1
        return nodes_to_be_added
        
    def __init__(self,
                settings: Settings):
        
        self.settings = settings

        self.embed_model = TextEmbeddingsInference(
            model_name="",
            base_url=self.settings.embedding.api_url,
            auth_token=self.settings.embedding.api_key.get_secret_value(),
            timeout=60,
            embed_batch_size=self.settings.embedding.embed_batch_size)
