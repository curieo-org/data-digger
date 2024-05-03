import asyncio
import argparse

from utils.utils import setup_logger
from database_vectordb_transform.database_to_vectors_engine import DatabaseVectorsEngine
from database_vectordb_transform.database_reader import PubmedDatabaseReader
from settings import Settings

settings = Settings()
logger = setup_logger("Main")

async def run_transform(year: int):
    #read the database
    dbReader = PubmedDatabaseReader(settings)
    if await dbReader.check_pubmed_percentile_tbl() and  2014 <= year <= 2024:
        records = await dbReader.collect_records_by_year(year)
    else:
        print("We cant process now because of the database problem")
        return
    
    #process the retrieved records
    dve = DatabaseVectorsEngine(settings)
    await dve.batch_process_records_to_vectors(records)

parser = argparse.ArgumentParser(description="Process records from PubMed database for a given year.")
parser.add_argument("year", type=int, help="The year for which to process records.", default=2034)
args = parser.parse_args() 
asyncio.run(run_transform(args.year))