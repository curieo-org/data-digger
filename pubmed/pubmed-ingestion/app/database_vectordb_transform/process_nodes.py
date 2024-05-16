import asyncio
from collections import defaultdict
from typing import List, Union
from sqlalchemy import create_engine
from loguru import logger
from tqdm import tqdm
from tqdm.asyncio import tqdm
import time

from llama_index.core.schema import (
    BaseNode,
    Document,
    TextNode
)
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.ingestion import run_transformations

from settings import Settings
from database_vectordb_transform.treefy_nodes import TreefyNodes
from database_vectordb_transform.vectors_upload import VectorsUpload
from utils.process_jats import JatsXMLParser
from utils.database_utils import run_insert_sql
from utils.utils import ProcessingResultEnum, update_result_status, download_s3_file

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class ProcessNodes:
    def __init__(self,
                settings: Settings):
        """
        Initializes the PubmedDatabaseReader with necessary settings and configurations.
        
        Args:
            settings (Settings): Contains all necessary database configurations.
        """
        self.settings = settings
        self.processed_records = defaultdict(list)
        self.children_pubmed_ids = []
        self.fulltext_pmc_sources = defaultdict(list)
        self.num_workers = 32
        self.engine = create_engine(self.settings.psql.connection.get_secret_value())
        self.tn = TreefyNodes(settings)
        self.vu = VectorsUpload(settings)

    async def parse_clean_fulltext(
            self,
            fulltext: str,
            name: str,
            split_depth: int):
        jatParser = JatsXMLParser(name=name, xml_data=fulltext)
        parsed_details = jatParser.parse_root_node()

        to_be_processed_sections = [
            section for section in parsed_details.get('body_sections', [])
            if section.get('title') and section['title'] not in ["Title", "Abstract"]
        ]

        # Process paragraphs
        grouped_ids = defaultdict(list)
        for section in to_be_processed_sections:
            id_value = section.get('id', '')
            prefix = '.'.join(id_value.split('.')[:split_depth]) if '.' in id_value else id_value
            paragraph_texts = [
                content.get('text') for content in section.get('contents', []) 
                if content.get('tag') == "p"
            ]
            grouped_ids[prefix].extend(paragraph_texts)

        return grouped_ids

    async def generate_children_nodes(
            self,
            fulltext_content: str,
            file_name: str
    ) -> defaultdict:
        """
        Asynchronously generates children nodes from a specified JATS text content by parsing it
        and applying transformations.

        Parameters:
            fulltext_content: the full text of the file in JATS form
            file_name: file name of the content

        Returns:
            Dict[str, List[Document]]: A dictionary mapping sections of the parsed text to lists of
            Document objects representing children nodes.

        Usage:
            children_dict = await self.generate_children_nodes(fulltext_content, file_name)
        """
        cur_children_dict = defaultdict(list)
        parsed_fulltext = await self.parse_clean_fulltext(fulltext=fulltext_content, name=file_name, split_depth=self.settings.jatsparser.split_depth)
            
        #process the retrieved data
        for k, values in parsed_fulltext.items():
            base_nodes: List[Document] = [Document(text=each_value) for each_value in values if each_value.strip()]
            cur_children_dict[k] = run_transformations(base_nodes, transformations=[
                SentenceSplitter(chunk_size=self.settings.d2vengine.child_chunk_size, chunk_overlap=self.settings.d2vengine.child_chunk_overlap)
                ],
                in_place=False)
        return cur_children_dict, parsed_fulltext
    
    async def node_metadata_transform(self, parent_id, pubmedid, nodes_to_be_added, record, only_children_push) -> list[BaseNode]:
        result: list[TextNode] = []
        keys_to_update = []
        if not only_children_push:
            keys_to_update = ["publicationDate", "year", "authors"]
            for node in nodes_to_be_added:
                for key in keys_to_update:
                    node.metadata[key] = record.get(key, "")

        #metadata update for the nodes
        excluded_keys = ["pubmedid", "level", "node_level", "cluster_id", "parent_id"] + keys_to_update
        for node in nodes_to_be_added:
            node.metadata["pubmedid"] = pubmedid
            node.metadata["parent_id"] = parent_id 
            
            for key in excluded_keys:
                node.excluded_embed_metadata_keys.append(key)
                node.excluded_llm_metadata_keys.append(key)
            result.append(node)
        return result

    async def process_single_record(self,
                                    record: Union[tuple, dict],
                                    only_children_push: bool = False):
        """
        This function downloads text content from an S3 bucket, parses the text to extract meaningful
        sections, and then applies transformations to generate child nodes.

        Parameters:
            record
            only_children_push (bool): push the children
        """
        children_nodes = []
        parent_nodes = []
        cur_children_dict = defaultdict(list)
        if only_children_push:
            id = record[0]
            parent_id = record[1]
        else:
            id = int(record.get('identifier'))
            if id <= 0 or len(record.get(self.settings.database_reader.parsed_record_abstract_key)) == 0:
                self.log_dict.append(
                    update_result_status(pubmed_id=id, status=ProcessingResultEnum.ID_ABSTRACT_BAD_DATA.value)
                ) 
                return    

            abstract = ",".join([abstract["string"] for abstract in record.get(self.settings.database_reader.parsed_record_abstract_key)])
            if len(abstract) == 0:
                self.log_dict.append(
                    update_result_status(pubmed_id=id, status=ProcessingResultEnum.ID_ABSTRACT_BAD_DATA.value)
                ) 
                return  

            # `Retrieve the parent nodes here
            if not only_children_push:
                parent_base_nodes: List[Document] = [Document(text=abstract)]
                parent_nodes = run_transformations(
                    nodes=parent_base_nodes,
                    transformations=[
                        SentenceSplitter(chunk_size=self.settings.d2vengine.parent_chunk_size, chunk_overlap=self.settings.d2vengine.parent_chunk_overlap)
                    ],
                    in_place=False
                )
                parent_id = parent_nodes[0].id_

        # Need to retrieve the children nodes here
        if id in self.children_pubmed_ids:
            # if there is a PMC source for this record
            if self.fulltext_pmc_sources.get(id):
                s3_loc = "bulk/" + self.fulltext_pmc_sources.get(id)
                fulltext_content = download_s3_file(self.settings.jatsparser.bucket_name, s3_object=s3_loc)

                # here we compute the clusters
                # then for each cluster we compute the centroids
                # we push the children to the PG database
                # we push the cluster to the cluster vector DB
                if fulltext_content:
                    cur_children_dict, parsed_fulltext = await self.generate_children_nodes(fulltext_content, s3_loc.split("/")[-1])
                    children_nodes = [item for sublist in cur_children_dict.values() for item in sublist]
                else:
                    self.log_dict.append(
                        update_result_status(
                            pubmed_id=id,
                            status=ProcessingResultEnum.PMC_RECORD_PARSING_FAILED.value,
                            parent_id_nodes_count=len(parent_nodes),
                            parent_id=parent_id,
                            children_nodes_count=len(children_nodes),
                            parsed_fulltext={}
                        )
                    )
            else:
                self.log_dict.append(
                    update_result_status(
                        pubmed_id=id,
                        status=ProcessingResultEnum.PMC_RECORD_NOT_FOUND.value,
                        parent_id_nodes_count=len(parent_nodes),
                        parent_id=parent_id,
                        children_nodes_count=len(children_nodes),
                        parsed_fulltext={}
                    )
                )

        #all nodes operation
        nodes = await self.tn.tree_transformation(parent_nodes, children_nodes, cur_children_dict)
        nodes_ready_to_b_added = await self.node_metadata_transform(parent_id, id, nodes, record, only_children_push)

        try:
            self.vu.index.insert_nodes(nodes_ready_to_b_added)
            # best Case - Everything is good
            self.log_dict.append(
                update_result_status(
                    pubmed_id=id,
                    status=ProcessingResultEnum.SUCCESS.value,
                    parent_id_nodes_count=len(parent_nodes),
                    parent_id=parent_id,
                    children_nodes_count=len(children_nodes),
                    parsed_fulltext={} if len(children_nodes) == 0 else parsed_fulltext
                )
            )
        except Exception as e:
            logger.exception(f"VectorDb problem: {e}")
            self.log_dict.append(
                update_result_status(pubmed_id=id, status=ProcessingResultEnum.VECTORDB_FAILED.value)
            ) 
        
    async def process_batch_records(self,
                                    records: list[defaultdict],
                                    only_children_push: bool = False,) -> None:
        jobs = []
        for each_record in records:           
            jobs.append(
                self.process_single_record(each_record, only_children_push)
            )

        lock = asyncio.Semaphore(self.num_workers)
        # run the jobs while limiting the number of concurrent jobs to num_workers
        for job in jobs:
            async with lock:
                await job

    async def batch_process_records_to_vectors(self,
                                               records: defaultdict, 
                                               batch_size:int = 100):
        self.parent_records = records.get("parent_records", {})
        self.children_pubmed_ids = records.get("children_pubmed_ids", [])
        self.fulltext_pmc_sources = records.get("fulltext_pmc_sources", {})

        keys = list(self.parent_records.keys())
        total_batches = (len(keys) + batch_size - 1) // batch_size
        for i in tqdm(range(total_batches), desc="Processing batches"):
            self.log_dict = []
            start_index = i * batch_size
            end_index = start_index + batch_size
            batch_data = [self.parent_records[key] for key in keys[start_index:end_index]]

            start_time = time.time()
            await self.process_batch_records(batch_data)
            logger.info(f"Processed Batch size of {batch_size} in {time.time() - start_time:.2f}s")
            run_insert_sql(engine=self.engine,
                                 table_name=self.settings.database_reader.pubmed_ingestion_log,
                                 data_dict=self.log_dict)

        logger.info(f"Processed Completed!!!")     

    async def batch_process_children_records_to_vectors(self,
                                                        records,
                                                        batch_size=100):
        self.children_pubmed_ids = records.get("children_pubmed_ids")
        self.parent_nodes_ids = records.get("parent_nodes_ids")
        self.fulltext_pmc_sources = records.get("fulltext_pmc_sources")

        assert len(self.children_pubmed_ids) == len(self.parent_nodes_ids)
        combined_ids = list(zip(self.children_pubmed_ids, self.parent_nodes_ids))

        total_batches = (len(combined_ids) + batch_size - 1) // batch_size
        for i in tqdm(range(total_batches), desc="Processing batches"):
            self.log_dict = []
            start_index = i * batch_size
            end_index = start_index + batch_size
            batch_data = combined_ids[start_index:end_index]
            start_time = time.time()
            await self.process_batch_records(batch_data, True)
            logger.info(f"Processed Batch size of {batch_size} in {time.time() - start_time:.2f}s")
            run_insert_sql(engine=self.engine,
                                 table_name=self.settings.database_reader.pubmed_ingestion_log,
                                 data_dict=self.log_dict)

        logger.info(f"Processed Completed!!!") 
