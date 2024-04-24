from collections import defaultdict
from enum import Enum
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor
import uuid
from tqdm import tqdm
from functools import partial

from llama_index.vector_stores.qdrant import QdrantVectorStore
from llama_index.core import StorageContext, ServiceContext
from llama_index.core.schema import (
    BaseNode,
    NodeWithScore,
    TextNode,
    Document
)
from llama_index.readers.database import DatabaseReader
from llama_index.core import VectorStoreIndex
from llama_index.readers.web import TrafilaturaWebReader
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference
from llama_index.core.node_parser import SentenceSplitter
import llama_index.core.instrumentation as instrument
from llama_index.core.ingestion import run_transformations

from sqlalchemy import create_engine, text
from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models
from typing import Any, List, Tuple
import asyncio

from utils.clustering import get_clusters
from utils.process_jats import JatsXMLParser
from utils.splade_embedding import SpladeEmbeddingsInference
from utils.custom_basenode import CustomBaseNode
from app.settings import Settings

dispatcher = instrument.get_dispatcher(__name__)

class BaseNodeTypeEnum(Enum):
    Parentnode = "parentnode"
    ChildNode = "childnode"

        
class DatabaseVectorsEngine:
    async def parse_clean_fulltext(
            self,
            fulltext,
            split_depth) -> dict:
        jatParser = JatsXMLParser(s3_bucket=self.s3_bucket, s3_object=fulltext)
        parsed_details = jatParser.parse_root_node()

        body_sections = parsed_details.get('body_sections', [])
        to_be_processed_sections = [
            section for section in body_sections
            if section.get('title', '') and section['title'] not in ["Title", "Abstract"]
        ]

        grouped_ids = defaultdict(list)

        for section in to_be_processed_sections:
            id_value = section.get('id', '')
            prefix = '.'.join(id_value.split('.')[:split_depth]) if '.' in id_value else id_value
            paragraph_texts = [
                content.get('text') for content in section.get('contents', []) if content.get('tag') == "p"
            ]
            grouped_ids[prefix].extend(paragraph_texts)

        return grouped_ids
    
    async def run_nodes_transformations(
            self,
            abstract_list: List[str], 
            fulltext_dict:defaultdict = defaultdict(str)
    ) -> Tuple[List[Document], defaultdict[str, List]]:
        cur_children_dict = defaultdict(list)
        parent_base_nodes: List[Document] = [Document(text=abs_text) for abs_text in abstract_list]
        
        cur_parent_nodes = run_transformations(
            nodes=parent_base_nodes,
            transformations=[
                SentenceSplitter(chunk_size=self.parent_chunk_size, chunk_overlap=self.parent_chunk_overlap)
            ],
            in_place=False
        )

        for k, values in fulltext_dict.items():
            base_nodes: List[Document] = [Document(text=each_value) for each_value in values]
            transformations = [
                SentenceSplitter(
                    chunk_size=self.child_chunk_size,
                    chunk_overlap=self.child_chunk_overlap
                    )
                ]
            cur_children_dict[k] = run_transformations(base_nodes, transformations, in_place=False)

        return cur_parent_nodes, cur_children_dict

    #@dispatcher.span
    async def ainsert_single_record(
            self,
            id: int,
            abstract: str,
            fulltext: str = "",
            split_depth: int = 2,
            **kwargs: Any
        ) -> None:
        """Will Add

        Args:
            documents (List[BaseNode]): List of Documents
        """
        #if fulltext exist - parse fulltext
        parsed_fulltext = await self.parse_clean_fulltext(fulltext, split_depth) if len(fulltext) > 0 else []
        p_nodes, c_dict = await self.run_nodes_transformations([abstract], parsed_fulltext)
        c_nodes = [item for sublist in c_dict.values() for item in sublist]
        parent_id = p_nodes[0].id

        all_cur_nodes = p_nodes + c_nodes
  
        embed_model = TextEmbeddingsInference(
            model_name="",
            base_url=self.text_embedding_url,
            auth_token=self.text_embedding_token,
            timeout=60,
            embed_batch_size=self.text_embedding_batch_size)
        
        sparse_model = SpladeEmbeddingsInference(
            model_name="",
            base_url=self.splade_doc_embedding_url,
            auth_token=self.splade_doc_embedding_token,
            timeout=60,
            embed_batch_size=self.splade_doc_embedding_batch_size)
        
        dense_embeddings = await embed_model.aget_text_embedding_batch(
            [node.get_content(metadata_mode="embed") for node in all_cur_nodes], show_progress=True
            )
        id_to_dense_embedding = {
            node.id_: dense_embedding for node, dense_embedding in zip(all_cur_nodes, dense_embeddings)
        }
        
        splade_embeddings = await sparse_model.aget_text_embedding_batch(
            [node.get_content(metadata_mode="embed") for node in all_cur_nodes], show_progress=True
            )
        id_to_splade_embedding = {
            node.id_: sparse_embedding for node, sparse_embedding in zip(all_cur_nodes, splade_embeddings)
        }

        cur_level = self.tree_depth - 1
        nodes_to_be_added = []
        while cur_level >= 0:
            
            #set parent node
            if cur_level == 1:
                for p_node in p_nodes:
                    cur_node = p_node
                    cur_node.metadata["level"] = 1
                    cur_node.metadata["node_type"] = BaseNodeTypeEnum.Parentnode
                    nodes_to_be_added.append(cur_node)
            #set children nodes
            elif cur_level == 0:
                if len(c_nodes) > 0:
                    #cluster create 

                    for k, children_section_nodes in c_dict.items():                 
                        children_section_nodes_ids = [node.id for node in children_section_nodes]
                        cur_id_to_dense_embedding = {k: v for k, v in id_to_dense_embedding.items() if k in children_section_nodes_ids}

                        nodes_per_cluster = get_clusters(
                            children_section_nodes,
                            cur_id_to_dense_embedding
                        )
                    
                        for cluster, summary_doc in zip(nodes_per_cluster, children_section_nodes):
                            current_cluster_id = str(uuid.uuid4())
                            for cur_node in cluster:
                                cur_node.metadata["level"] = 0
                                cur_node.metadata["node_type"] = BaseNodeTypeEnum.ChildNode
                                cur_node.metadata["cluster_id"] = current_cluster_id
                                nodes_to_be_added.append(cur_node)
        cur_level = cur_level - 1

        for node in nodes_to_be_added:  
            node.metadata["parent_id"] = parent_id 
            node.embedding = id_to_dense_embedding[node.id_]
            node.splade_embedding = id_to_splade_embedding[node.id_]

            node.excluded_embed_metadata_keys.append("level")
            node.excluded_llm_metadata_keys.append("level")

            node.excluded_embed_metadata_keys.append("node_type")
            node.excluded_llm_metadata_keys.append("node_type")

            node.excluded_embed_metadata_keys.append("cluster_id")
            node.excluded_llm_metadata_keys.append("cluster_id")

            node.excluded_embed_metadata_keys.append("parent_id")
            node.excluded_llm_metadata_keys.append("parent_id")
        
        self.index.insert_nodes(nodes_to_be_added)

    async def process_batch_records(self, records: defaultdict[BaseNode]) -> None:

        jobs = []
        for each_record in records:           
            jobs.append(
                self.ainsert_single_record(
                    id=each_record.get('id'),
                    abstract=each_record.get('abstract'),
                    fulltext=each_record.get('fulltext', ""),
                    split_depth=self.section_split_depth
                )
            )

        lock = asyncio.Semaphore(self.num_workers)

        # run the jobs while limiting the number of concurrent jobs to num_workers
        for job in jobs:
            async with lock:
                await job

    async def database_to_vectors(self):
        # reader = PubmedDatabaseReader(engine=create_engine(self.settings.postgres_engine.get_secret_value()))
        # count = 0
        # for batch in tqdm(reader.load_data_in_batches(query_template=self.query_template, batch_size=self.batch_size)):
        #     self.process_records(list(batch), chunk_size=self.chunk_size)
        #     count += len(batch)
        #     print("Processed count  = %d" % count)

        records = [
            {
                "id": 12345,
                "abstract": "Tegument proteins of herpes simplex virus type 1 (HSV-1) are hypothesized to contain the functional information required for the budding or envelopment process proposed to occur at cytoplasmic compartments of the host cell. One of the most abundant tegument proteins of HSV-1 is the U(L)49 gene product, VP22, a 38-kDa protein of unknown function. To study its subcellular localization, a VP22-green fluorescent protein chimera was expressed in transfected human melanoma (A7) cells. In the absence of other HSV-1 proteins, VP22 localizes to acidic compartments of the cell that may include the trans-Golgi network (TGN), suggesting that this protein is membrane associated. Membrane pelleting and membrane flotation assays confirmed that VP22 partitions with the cellular membrane fraction. Through truncation mutagenesis, we determined that the membrane association of VP22 is a property attributed to amino acids 120 to 225 of this 301-amino-acid protein. The above results demonstrate that VP22 contains specific information required for targeting to membranes of acidic compartments of the cell which may be derived from the TGN, suggesting a potential role for VP22 during tegumentation and/or final envelopment.",
                "fulltext" : "fulltext/2024/journal.pone.0292755.xml"
            }
        ]
        
        await self.process_batch_records(records=records)

    def __init__(self,
                settings: Settings):
        
        self.settings = settings
        self.num_workers = 4

        self.s3_bucket  = self.settings.jetsparser.bucket_name
        self.section_split_depth = self.settings.jetsparser.section_split_depth
        
        self.parent_chunk_size = self.settings.d2vengine.parent_chunk_size
        self.parent_chunk_overlap = self.settings.d2vengine.parent_chunk_overlap
        self.child_chunk_size = self.settings.d2vengine.child_chunk_size
        self.child_chunk_overlap = self.settings.d2vengine.child_chunk_overlap
        self.tree_depth = self.settings.d2vengine.tree_depth

        self.text_embedding_url = self.settings.embedding.api_url
        self.text_embedding_token = self.settings.embedding.api_key.get_secret_value()
        self.text_embedding_batch_size = self.settings.embedding.embed_batch_size

        self.splade_doc_embedding_url = self.settings.spladedoc.api_url
        self.splade_doc_embedding_token = self.settings.spladedoc.api_key.get_secret_value()
        self.splade_doc_embedding_batch_size = self.settings.spladedoc.embed_batch_size

        self.qdrant_url_address = self.settings.qdrant.api_url
        self.qdrant_url_port = self.settings.qdrant.api_port
        self.qdrant_collection_name = self.settings.qdrant.collection_name
        self.qdrant_api_key = self.settings.qdrant.api_key.get_secret_value()

        self.aclient = AsyncQdrantClient(
            url=self.qdrant_url_address,
            port=self.qdrant_url_port, 
            api_key=self.qdrant_api_key,
            https=False
            )
        
        self.aclient.recreate_collection(
            collection_name=self.qdrant_collection_name,
            vectors_config={
                "text-dense": models.VectorParams(
                    size=1024,
                    distance=models.Distance.COSINE,
                )
            },
            sparse_vectors_config={
                "text-sparse": models.SparseVectorParams(
                    index=models.SparseIndexParams(
                        on_disk=False,
                        )
                    )
                }
            )
        
        self.vector_store = QdrantVectorStore(
            aclient=self.aclient,
            collection_name=self.qdrant_collection_name,
            enable_hybrid=True
        )

        self.service_context = ServiceContext.from_defaults(
            embed_model=TextEmbeddingsInference(
                model_name="",
                base_url="http://text-embedding.dev.curieo.org/embed",
                auth_token="Bearer e9260789-8d00-42b5-bf11-034e76eba43d",
                timeout=60, embed_batch_size=24),
            llm=None
        )
        self.storage_context = StorageContext.from_defaults(vector_store=self.vector_store)
        
        self.index = VectorStoreIndex(
            nodes=[],
            storage_context=self.storage_context
        )
        




        