import asyncio
from .database_transfer import transfer_all_tables

async def async_main():
    print('Starting the data transfer process')
    await transfer_all_tables()

def main():
    asyncio.run(async_main())