import asyncio

from app.ingestion import ingest_all_tables
from app.settings import Settings

settings = Settings()


async def async_main():
    print("Starting the data ingestion process")
    await ingest_all_tables(settings)
    print("Data ingestion completed")


def main():
    asyncio.run(async_main())
