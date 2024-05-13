import asyncio
import argparse
from typing import List
from loguru import logger

from database_vectordb_transform.database_to_vectors_engine import DatabaseVectorsEngine
from database_vectordb_transform.database_reader import PubmedDatabaseReader
from settings import Settings

settings = Settings()
logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

async def run_transform(commands: argparse.Namespace):
    #read the database
    dbReader = PubmedDatabaseReader(settings)
    logger.bind(special=True).info("Starting the INGESTION Process!!!")
    
    if await dbReader.check_pubmed_percentile_tbl():
        if commands.parentcriteria <= commands.childcriteria:
            records = await dbReader.collect_records_by_year(
                year=commands.year, 
                parent_criteria=commands.parentcriteria, 
                child_criteria=commands.childcriteria)
        else:
            logger.debug("Please look the criteria - Child Criteria should be always greater than Parent Criteria!!")
            return
    else:
        logger.debug("Database is not ready to read yet!!")
        return
    
    #process the retrieved records
    dve = DatabaseVectorsEngine(settings)
    await dve.batch_process_records_to_vectors(records, commands.year)

def parse_args(commands: List[str] = None) -> argparse.Namespace:
    """
    Parse command line arguments
    :param commands: to provide command line programmatically
    :return: parsed command line
    """
    parser = argparse.ArgumentParser(
        description="Process records from PubMed database for a given year.", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-y", "--year", type=int, required=True, help="year to process record", default=2024)
    parser.add_argument("-pc", "--parentcriteria", type=int, required=True, help="Parent Criteria to process", default=100)
    parser.add_argument("-cc", "--childcriteria", type=int, required=True, help="Child Criteria to process", default=100)
    args, _ = parser.parse_known_args(args=commands)
    return args
    
def entrypoint():
    args = parse_args()
    asyncio.run(run_transform(commands=args))

if __name__ == "__main__":
    entrypoint()
