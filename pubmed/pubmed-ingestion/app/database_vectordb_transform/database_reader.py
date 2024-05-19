from collections import defaultdict
from typing import Any, Dict, List, Union
import json
from sqlalchemy import create_engine, inspect, text
from loguru import logger
from tqdm import tqdm
from tqdm.asyncio import tqdm

from settings import Settings
from database_vectordb_transform.process_parent_nodes import ProcessParentNodes
from database_vectordb_transform.process_children_nodes import ProcessChildrenNodes
from utils.database_utils import list_to_sql_tuple, run_select_sql

logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

class PubmedDatabaseReader:
    """
    Handles database interactions specifically tailored to retrieve and process
    PubMed dataset records based on various criteria including year and percentile.
    
    Attributes:
        settings (Settings): Configuration settings for database connections and queries.
        engine (AsyncEngine): SQLAlchemy engine instance for database operations.
    """
    def __init__(self,
                settings: Settings):
        """
        Initializes the PubmedDatabaseReader with necessary settings and configurations.
        
        Args:
            settings (Settings): Contains all necessary database configurations.
        """
        self.settings = settings
        self.pn = ProcessParentNodes(settings)
        self.cn = ProcessChildrenNodes(settings)
        self.engine = create_engine(self.settings.psql.connection.get_secret_value())
        self.db_fetch_batch_size: int = 500
        self.year = 1900

    async def check_pubmed_percentile_tbl(self) -> bool:
        try:
            exists = inspect(self.engine).has_table(self.settings.database_reader.pubmed_percentiles_tbl) 
            logger.info("check_pubmed_percentile_tbl. table exists: " + str(exists))
            return exists
        except Exception as e:
            logger.exception(f"Error checking table existence: {e}")
            return False

    def fetch_details(self,
                            ids,
                            query_template,
                            json_parse_required,
                            **params)-> Dict[int, Any]:
        all_records = defaultdict(list)
        if len(ids):
            for i in tqdm(range(0, len(ids)), desc="Gathering full records batch"):
                formatted_ids = list_to_sql_tuple(ids)
                
                query = query_template.format(year=self.year, ids=formatted_ids, **params)
                try:
                    result = run_select_sql(self.engine, query)
                    for id, record in result.get('result'):
                        if json_parse_required:
                            all_records[id]= json.loads(record)
                        else:
                            all_records[id]= str(record)
                    
                except Exception as e:
                    logger.exception(f"An error occurred while fetching records: {e}")
            return all_records
        else:
            return {}
        
    async def process_batch_records(
        self,
        ids: List[str],
        mode: str = Union["parent", "children"]
    ) -> None:
        if mode == "parent":
            database_records = self.fetch_details(
                ids=ids,
                query_template=self.settings.database_reader.records_fetch_details,
                json_parse_required=True
            )
            await self.pn.batch_process_records_to_vectors(database_records, 100)
        else:
            database_records = self.fetch_details(
                ids=ids,
                query_template=self.settings.database_reader.records_fetch_details,
                json_parse_required=False
            )
            await self.cn.batch_process_records_to_vectors(database_records, 100)

    async def collect_records_by_year(
        self,
        year: int = 1900,
        lowercriteria: int = 0,
        highercriteria: int = 0,
        mode: str = Union["parent", "children"]
    ) -> Dict[int, Any]:
        query_template = self.get_query_template(mode)
        self.year = year
        parent_query = self.format_parent_query(query_template, lowercriteria, highercriteria)
        
        logger.info(f"collect_records_by_year: Year: {year}: lowercriteria: {lowercriteria} : highercriteria: {highercriteria}")
        with self.engine.connect() as conn:
            with conn.execution_options(stream_results=True, max_row_buffer=self.db_fetch_batch_size).execute(
                text(parent_query)
            ) as result:
                logger.info(f"collect_records_by_year: Year: {year}: Streaming Started")
                count = 0
                for partition in result.partitions():
                    logger.info(f"To be processed Parent Processed Records: {len(partition)}")
                    ids = []
                    for row in partition:
                        ids.append(row[0])
                        if len(ids) < self.db_fetch_batch_size:
                            continue

                        await self.process_batch_records(ids, mode)
                        count = count + self.db_fetch_batch_size
                        logger.info(f"Parent Processed Records {count}")
                        ids = []

                    if len(ids) > 0:
                        await self.process_batch_records(ids, mode)
                        count = count + len(ids)
                        logger.info(f"Parent Processed Records {count}")

    def get_query_template(self, mode: str) -> str:
        if mode == "parent":
            return self.settings.database_reader.pubmed_fetch_parent_records
        elif mode == "children":
            return self.settings.database_reader.pubmed_fetch_children_records
        raise ValueError("Unsupported mode")

    def format_parent_query(self, query_template: str, lowercriteria: int, highercriteria: int) -> str:
        return query_template.format(
            year=self.year,
            parent_criteria_upper=100 - highercriteria,
            parent_criteria_lower=100 - lowercriteria
        )
