
import asyncio
from collections import defaultdict
from typing import Any, Dict, List
import json
from sqlalchemy import create_engine, inspect
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from tqdm.asyncio import tqdm

from settings import Settings
from utils.database_utils import run_sql

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
        self.processed_records = defaultdict(list)
        self.num_workers = 32

    async def check_pubmed_percentile_tbl(self) -> bool:
        """
        Asynchronously checks if the 'pubmed_percentiles_tbl' exists in the database.

        Returns:
            bool: True if the table exists, False otherwise.

        Raises:
            Exception: Outputs an error message to the console if an exception occurs.
        """
        try:
            exists = inspect(self.engine).has_table(self.settings.database_reader.pubmed_percentiles_tbl) 
            logger.info("check_pubmed_percentile_tbl. table exists: " + str(exists))
            return exists
        except Exception as e:
            logger.exception(f"Error checking table existence: {e}")
            return False
        
    async def get_percentile_count(self, year: int, criteria: int) -> int:
        """
        Asynchronously retrieves a count based on a specified year and adjusted percentile criteria
        from the database. This function calculates the percentile as 100 minus the given criteria.

        Args:
            year (int): The year to filter the records by.
            criteria (int): The criteria used to compute the percentile (100 - criteria).

        Returns:
            int: The count of records that meet the specified percentile criteria for the given year.

        Example:
            # Fetch the count of records for the year 2021 with criteria resulting in a percentile of 80
            count = await get_percentile_count(2021, 20)
        """
        query = self.settings.database_reader.percentile_select_query.format(year=year, percentile=100 - criteria)
        count = 0
        try:
            result = run_sql(self.engine, query)
            count = result['result'][0][0] if result['result'] else 0
        except Exception as e:
            logger.exception(f"An error occurred while fetching the percentile count: {e}")
        return count
            
    async def get_records_by_year_criteria(self,
                                            year: int,
                                            criteria: int) -> List[int]:
        """
        Asynchronously retrieves records based on a specified year and criteria,
        such as a citation count, from the database.

        Args:
            year (int): The year to filter the records by.
            criteria (int): The criteria value to filter the records by, e.g., citation count.

        Returns:
            List[int]: A list of record identifiers that match the given year and criteria.

        Example:
            # Fetch records for the year 2021 with a citation count of 100
            record_ids = await get_records_by_year_criteria(2021, 100)
        """
        query = self.settings.database_reader.record_select_query.format(year=year, citationcount=criteria)
        ids = []
        try:
            result = run_sql(self.engine, query)
            if result.get('result'):
                ids = [item[0] for item in result.get('result')]
        except Exception as e:
            logger.exception(f"An error occurred while fetching records: {e}")
        return ids

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
                    result = run_sql(self.engine, query)
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
        
    async def process_single_record(self, id, details, fulltext_pmc_sources, children_ids):
        """
        Process a single PubMed record based on the provided details, fulltext_pmc_sources, and children_ids.
    
        Args:
            id (int): The unique identifier for the PubMed record.
            details (dict): A dictionary containing parsed details about the PubMed record.
            fulltext_pmc_sources (dict): A dictionary containing fulltext locations for PubMed records.
            children_ids (list): A list of child record identifiers.

        Returns:
            dict: A dictionary containing processed details about the PubMed record.

        Raises:
            Exception: If an error occurs while processing the record.

        Example:
            # Process a single PubMed record
            processed_record = await process_single_record(12345, details, fulltext_pmc_sources, [67890])
        """
        self.processed_records[id] = {
            "id": id,
            "abstract": ",".join([abstract["string"] for abstract in details.get(self.settings.database_reader.parsed_record_abstract_key)]),
            "title": ",".join([abstract["string"] for abstract in details.get(self.settings.database_reader.parsed_record_titles_key)]),
            "publicationDate": details.get(self.settings.database_reader.parsed_record_publicationdate_key),
            "year": details.get(self.settings.database_reader.parsed_record_year_key),
            "authors": details.get(self.settings.database_reader.parsed_record_authors_key),
            "references": {item['identifiers'].get('pubmed'): item['citation'] for item in details.get(self.settings.database_reader.parsed_record_references_key) or []},
            "identifiers": {item['key']: item['value'] for item in details.get(self.settings.database_reader.parsed_record_identifiers_key) or []},
            "fulltext_s3_loc": fulltext_pmc_sources.get(id, ""),
                "fulltext_to_be_parsed": str(id) in children_ids if fulltext_pmc_sources.get(id, "") else False #if the id is in the top nodes, then we should parse the fulltext later
            }
      
    async def collect_records_by_year(self,
                                      year:int,
                                      parent_criteria:int,
                                      child_criteria:int) -> Dict[int, Any]:
        """
        Asynchronously collects and processes records for a given year from a database,
        categorizing them into parent and child records based on predefined criteria
        and then fetching detailed information for each.

        Args:
            year (int): The year for which records need to be collected and processed.
            parentcriteria (int) : The parent criteria for which records need to be collected
            childcriteria (int) : The child criteria for which records need to be collected

        Returns:
            DefaultDict[Any, Dict[str, Any]]: A default dictionary where each key is an
            identifier for a pubmed record, and the value is another dictionary containing
            processed details about the pubmed record such as abstract, title, publication date,
            authors, references, and fulltext location.
        """
        logger.info(f"collect_records_by_year. Year: {year}. parent_criteria: {parent_criteria}")
        logger.info(f"collect_records_by_year. Year: {year}. child_criteria: {child_criteria}")

        parent_percentile_count, child_percentile_count = await asyncio.gather(
            self.get_percentile_count(year, parent_criteria),
            self.get_percentile_count(year, child_criteria)
        )
        logger.info(f"collect_records_by_year. Year: {year}. parent_percentile_count: {parent_percentile_count}")
        logger.info(f"collect_records_by_year. Year: {year}. child_percentile_count: {child_percentile_count}")

        parents_ids, children_ids = await asyncio.gather(
            self.get_records_by_year_criteria(year, parent_percentile_count),
            self.get_records_by_year_criteria(year, child_percentile_count)
        )
        logger.info(f"collect_records_by_year. Year: {year}. parents_ids Count: {len(parents_ids)}")
        logger.info(f"collect_records_by_year. Year: {year}. children_ids Count: {len(children_ids)}")

        parent_records, fulltext_pmc_sources = await asyncio.gather(
            self.fetch_details(parents_ids, self.settings.database_reader.records_fetch_details, True),
            self.fetch_details(parents_ids, self.settings.database_reader.fulltext_fetch_query, False, column="location", table="linktable")
        )

        logger.info(f"collect_records_by_year. Year: {year}. parent_records Count: {len(parent_records)}")
        logger.info(f"collect_records_by_year. Year: {year}. fulltext_pmc_sources Count: {len(fulltext_pmc_sources)}")

        jobs = []
        for key, value in parent_records.items():           
            jobs.append(self.process_single_record(key, value, fulltext_pmc_sources, children_ids))
        lock = asyncio.Semaphore(self.num_workers)
        
        # run the jobs while limiting the number of concurrent jobs to num_workers
        async def run_job(job):
            async with lock:
                await job

        await tqdm.gather(*(run_job(job) for job in jobs))
        
        logger.info(f"collect_records_by_year. Year: {year}. processed_records Count: {len(self.processed_records)}")
        return self.processed_records