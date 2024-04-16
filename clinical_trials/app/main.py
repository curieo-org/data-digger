import asyncio
from app.database_transfer import transfer_all_tables
from app.scraper import (
    scrape_clinical_trial_database,
    setup_local_database,
    remove_local_database
)
from app.vector_transfer import ClinicalTrailVectorDbEngine
from app.config import (
    PG_LOCAL_DATABASE,
    QDRANT_HOST,
    QDRANT_PORT,
    QDRANT_COLLECTION_NAME,
    QDRANT_API_KEY
)

async def async_main():
    print('Starting the scraping process and setting up the local database')
    #scrape_clinical_trial_database()
    #setup_local_database()

    print('Starting the data transfer process')
    await transfer_all_tables()

    print('Starting the vector generation and transfer process')
    ctve = ClinicalTrailVectorDbEngine(
        qdrant_url_address=QDRANT_HOST,
        qdrant_url_port=QDRANT_PORT,
        qdrant_collection_name=QDRANT_COLLECTION_NAME,
        qdrant_api_key=QDRANT_API_KEY
    )
    ctve.database_to_vectors(database_engine=PG_LOCAL_DATABASE)

    print('Removing the local database')
    remove_local_database()

def main():
    asyncio.run(async_main())
