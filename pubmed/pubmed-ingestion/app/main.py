import asyncio
import argparse
from typing import List
from loguru import logger

from database_vectordb_transform.database_reader import PubmedDatabaseReader
from database_vectordb_transform.process_nodes import ProcessNodes
from settings import Settings

settings = Settings()
logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

async def run_transform(commands: argparse.Namespace):
    #read the database
    dbReader = PubmedDatabaseReader(settings)
    pn = ProcessNodes(settings)
    logger.bind(special=True).info("Starting the INGESTION Process!!!")
    
    if await dbReader.check_pubmed_percentile_tbl():
        if commands.onlychildrenpush or commands.parentcriteria <= commands.childcriteria:
            records = await dbReader.collect_records_by_year(
                year=commands.year, 
                parent_criteria=commands.parentcriteria, 
                child_criteria=commands.childcriteria,
                only_children_push=commands.onlychildrenpush
            )
            process_method = pn.batch_process_children_records_to_vectors if commands.onlychildrenpush else pn.batch_process_records_to_vectors
            await process_method(records)
        else:
            logger.debug("Please look the criteria - Child Criteria should be always greater than Parent Criteria!!")
            return
    else:
        logger.debug("Database is not ready to read yet!!")
        return

def parse_args(commands: List[str] = None) -> argparse.Namespace:
    """
    Parse command line arguments
    :param commands: to provide command line programmatically
    :return: parsed command line
    """
    parser = argparse.ArgumentParser(
        description="Process records from PubMed database for a given year.", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-y", "--year", type=int, help="year to process record", default=2020)
    parser.add_argument("-pc", "--parentcriteria", type=int, help="Parent Criteria to process", default=98)
    parser.add_argument("-cc", "--childcriteria", type=int, help="Child Criteria to process", default=100)
    parser.add_argument("-ocp", "--onlychildrenpush", type=bool, help="Use This when you need to push only children nodes, parents nodes are already processed", default=False)
    args, _ = parser.parse_known_args(args=commands)
    return args
    
def entrypoint():
    args = parse_args()
    asyncio.run(run_transform(commands=args))

if __name__ == "__main__":
    entrypoint()
