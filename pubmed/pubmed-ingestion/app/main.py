import argparse
from typing import List
from loguru import logger

from database_vectordb_transform.database_reader import PubmedDatabaseReader
from settings import Settings

settings = Settings()
logger.add("file.log", rotation="500 MB", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

def run_transform(commands: argparse.Namespace):
    dbReader = PubmedDatabaseReader(settings)
    logger.bind(special=True).info("Starting the INGESTION Process 0.0.31!!!")
    
    if dbReader.check_pubmed_percentile_tbl():
        if commands.lowercriteria <= commands.highercriteria:
            dbReader.collect_records_by_year(
                year=commands.year,
                lowercriteria=commands.lowercriteria,
                highercriteria=commands.highercriteria,
                mode=commands.mode
            )
        else:
            logger.debug("Please look the criteria - Lower Criteria should be always greater than High Criteria!!")
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
    parser.add_argument("-y", "--year", type=int, help="year to process record", default=2016)
    parser.add_argument(
        "--mode",
        default="children",
        choices=["parent", "children"],
        help="mode to process records",
    )
    parser.add_argument("-hl", "--highercriteria", type=int, help="Higher PercentileCriteria to process", default=100)
    parser.add_argument("-ll", "--lowercriteria", type=int, help="Lower Percentile Criteria to process", default=90)
    args, _ = parser.parse_known_args(args=commands)
    return args
    
def entrypoint():
    args = parse_args()
    run_transform(commands=args)

if __name__ == "__main__":
    entrypoint()
