import asyncio
from collections import defaultdict
from typing import Any, Dict, List
import json
from sqlalchemy import create_engine, inspect
from loguru import logger
from tqdm import tqdm
from tqdm.asyncio import tqdm

from settings import Settings
from utils.database_utils import run_select_sql

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
        self.engine = create_engine(self.settings.psql.connection.get_secret_value())
        self.create_logs_table()

    def create_logs_table(self) -> bool:
        for query in self.settings.database_reader.pubmed_ingestion_log_queries:
            run_select_sql(self.engine, query)

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
            
    async def get_records_by_year_criteria(self,
                                            year: int,
                                            criteria: int,
                                            only_children_push: bool) -> List[int]:
        query_template = (self.settings.database_reader.pubmed_citation_ingested_log_filter_children_push_only
                          if only_children_push 
                          else self.settings.database_reader.pubmed_citation_ingested_log_filter)
        query = query_template.format(year=year, citationcount=criteria)
        ids = []
        try:
            result = run_select_sql(self.engine, query)
            if result.get('result'):
                ids = [item[0] for item in result.get('result')]
                if only_children_push:
                    parent_ids = [item[1]for item in result.get('result')]                   
        except Exception as e:
            logger.exception(f"An error occurred while fetching records: {e}")
        return (ids, parent_ids) if only_children_push else ids

    async def fetch_details(self,
                            ids,
                            query_template,
                            json_parse_required,
                            **params)-> Dict[int, Any]:
        """
        Asynchronously fetches details from the database and processes them based on the provided IDs and query template.
        Optionally parses JSON content from the database records.

        Args:
            ids (List[int]): List of IDs to fetch details for.
            query_template (str): SQL query template where `{ids}` will be replaced by the provided IDs and may include additional formatting.
            json_parse_required (bool): Indicates whether the returned records need to be JSON parsed.
            **params (dict): Additional parameters to format the SQL query template.

        Returns:
            Dict[int, Any]: A dictionary where each key is an ID and the value is the processed record, either as a string or a parsed JSON object.

        Example:
            # Example SQL template with additional parameters for table and column names
            sql_template = "SELECT {column} FROM {table} WHERE id IN ({ids})"
            results = await fetch_details([1, 2, 3], sql_template, True, table='my_table', column='data_column')
        """
        batch_size=500
        all_records = defaultdict(list)
        if len(ids):
            for i in tqdm(range(0, len(ids), batch_size), desc="Processing fetch_details batch"):
                batch_ids = ids[i:i + batch_size]
                formatted_ids = ",".join(map(str, map(int, batch_ids)))
                query = query_template.format(ids=formatted_ids, **params)
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

    async def collect_records_by_year(self,
                                      year:int = 1900,
                                      parent_criteria:int = 0,
                                      child_criteria:int = 0,
                                      only_children_push = False) -> Dict[int, Any]:
        """
        Asynchronously collects and processes records for a given year from a database,
        categorizing them into parent and child records based on predefined criteria
        and then fetching detailed information for each.

        Args:
            year (int): The year for which records need to be collected and processed.
            parent_criteria (int): The parent criteria for which records need to be collected.
            child_criteria (int): The child criteria for which records need to be collected.
            only_children_push (bool): If True, only collect child records and push them to the next stage.

        Returns:
            Dict[int, Any]: A dictionary where each key is an identifier for a pubmed record,
            and the value is another dictionary containing processed details about the pubmed record
            such as abstract, title, publication date, authors, references, and fulltext location.

        The dictionary keys are identifiers for the PubMed records, and the values are dictionaries
        containing processed details about the records. The structure of the returned dictionary
        depends on the parameters provided to the function. If `only_children_push` is True,
        the dictionary will contain only the identifiers of the child records. Otherwise, it will
        contain the identifiers of both parent and child records, along with their corresponding
        processed details.
        """
        parent_percentile_count, child_percentile_count = await asyncio.gather(
            self.get_percentile_count(year, parent_criteria),
            self.get_percentile_count(year, child_criteria)
        )

        if only_children_push:
            children_pubmed_ids, parent_nodes_ids = await self.get_records_by_year_criteria(year, child_percentile_count, only_children_push=only_children_push)
            fulltext_pmc_sources = await self.fetch_details(children_pubmed_ids, self.settings.database_reader.fulltext_fetch_query, False, column="location", table="linktable")
            
            logger.info(f"collect_records_by_year. Year: {year}. Total Children Nodes Count: {len(children_pubmed_ids)}")
            logger.info(f"collect_records_by_year. Year: {year}. Total fulltext_pmc_sources Nodes Count: {len(fulltext_pmc_sources)}")

            return {
                "children_pubmed_ids" : children_pubmed_ids if len(children_pubmed_ids) else [],
                "parent_nodes_ids" : parent_nodes_ids if len(parent_nodes_ids) else [],
                "fulltext_pmc_sources" : fulltext_pmc_sources if len(fulltext_pmc_sources) else {}
            }
        else: 
            parents_pubmed_ids, children_pubmed_ids = await asyncio.gather(
                self.get_records_by_year_criteria(year, parent_percentile_count, only_children_push=False),
                self.get_records_by_year_criteria(year, child_percentile_count, only_children_push=False)
            )

            parent_records, fulltext_pmc_sources = await asyncio.gather(
                self.fetch_details(parents_pubmed_ids, self.settings.database_reader.records_fetch_details, True),
                self.fetch_details(children_pubmed_ids, self.settings.database_reader.fulltext_fetch_query, False, column="location", table="linktable")
            )

            logger.info(f"collect_records_by_year. Year: {year}. Total Nodes Count Count: {len(parent_records)}")
            logger.info(f"collect_records_by_year. Year: {year}. fulltext_pmc_sources Count: {len(fulltext_pmc_sources)}")

            return {
                "parent_records" : parent_records if len(parent_records) else {},
                "children_pubmed_ids" : children_pubmed_ids if len(children_pubmed_ids) else [],
                "fulltext_pmc_sources" : fulltext_pmc_sources if len(fulltext_pmc_sources) else {}
            }
