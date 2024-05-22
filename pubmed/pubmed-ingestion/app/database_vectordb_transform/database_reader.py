from collections import defaultdict
from typing import Any, Dict, List, Union
import json
from sqlalchemy import create_engine, inspect, text
from loguru import logger
from tqdm import tqdm
from tqdm.asyncio import tqdm
import asyncio

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
        
    async def get_percentile_count(self, year: int, criteria: int) -> int:
        query = self.settings.database_reader.percentile_select_query.format(year=year, percentile=100 - criteria)
        count = 0
        try:
            result = run_select_sql(self.engine, query)
            count = result['result'][0][0] if result['result'] else 0
        except Exception as e:
            logger.exception(f"An error occurred while fetching the percentile count: {e}")
        return count

    async def fetch_details(self,
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
            database_records, pmc_sources = await asyncio.gather(
                self.fetch_details(
                    ids=ids,
                    query_template=self.settings.database_reader.records_fetch_details,
                    json_parse_required=True
                ),
                self.fetch_details(
                    ids=ids,
                    query_template=self.settings.database_reader.fulltext_fetch_query,
                    json_parse_required=False,
                    column="location",
                    table="linktable"
                )
            )
            await self.cn.batch_process_children_ids_to_vectors(database_records, pmc_sources, 100)

    async def collect_records_by_year(
        self,
        year: int = 1900,
        lowercriteria: int = 0,
        highercriteria: int = 0,
        mode: str = Union["parent", "children"]
    ) -> Dict[int, Any]:
        self.year = year

        #retrieve the citation count according to the lowercriteria and highercriteria
        lower_cc, higher_cc = await asyncio.gather(
            self.get_percentile_count(year, lowercriteria),
            self.get_percentile_count(year, highercriteria)
        )

        query = self.format_citation_query(mode, lower_cc, higher_cc)
        
        logger.info(f"collect_records_by_year: Model: {mode}, Year: {year}: lowercriteria: {lowercriteria} : highercriteria: {highercriteria}")
        with self.engine.connect() as conn:
            with conn.execution_options(stream_results=True, max_row_buffer=self.db_fetch_batch_size).execute(
                text(query)
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

    def format_citation_query(self, mode: str, citation_lower: int, citation_upper: int) -> str:
        query_template = self.settings.database_reader.pubmed_fetch_records
        table_name = (self.settings.database_reader.pubmed_parent_ingestion_log if mode == "parent"
                  else self.settings.database_reader.pubmed_children_ingestion_log)
        return query_template.format(
            table_name=table_name,
            year=self.year,
            citation_lower=citation_lower,
            citation_upper=citation_upper
        )
    