import asyncio
import argparse
from typing import List

from utils.utils import setup_logger
from database_vectordb_transform.database_to_vectors_engine import DatabaseVectorsEngine
from database_vectordb_transform.database_reader import PubmedDatabaseReader
from settings import Settings

settings = Settings()
logger = setup_logger("Main")

async def run_transform(commands: argparse.Namespace):
    #read the database
    dbReader = PubmedDatabaseReader(settings)
    if await dbReader.check_pubmed_percentile_tbl() and  2014 <= commands.year <= 2024:
        records = await dbReader.collect_records_by_year(commands.year)
    else:
        print("We cant process now because of the database problem")
        return
    
    #process the retrieved records
    dve = DatabaseVectorsEngine(settings)
    await dve.batch_process_records_to_vectors(records)

def parse_args(commands: List[str] = None) -> argparse.Namespace:
    """
    Parse command line arguments
    :param commands: to provide command line programmatically
    :return: parsed command line
    """
    parser = argparse.ArgumentParser(
        description="Process records from PubMed database for a given year.", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-y", "--year", required=True, type=int, help="year to process record", default=2024)
    args, _ = parser.parse_known_args(args=commands)
    return args
    
def entrypoint():
    args = parse_args()
    asyncio.run(run_transform(commands=args))


if __name__ == "__main__":
    entrypoint()
