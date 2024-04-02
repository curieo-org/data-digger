import asyncio
from app.database_transfer import transfer_all_tables
from app.scraper import scrape_clinical_trial_database, setup_local_database, remove_local_database

async def async_main():
    print('Starting the scraping process and setting up the local database')
    await scrape_clinical_trial_database()
    await setup_local_database()

    print('Starting the data transfer process')
    await transfer_all_tables()

    print('Removing the local database')
    await remove_local_database()

def main():
    asyncio.run(async_main())