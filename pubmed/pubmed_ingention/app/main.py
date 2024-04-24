import asyncio

from app.utils.setup_logger import setup_logger
from app.database_vectordb_transform.database_to_vectors_engine import DatabaseVectorsEngine
from app.settings import Settings

settings = Settings()
logger = setup_logger("Main")

async def run_transform():
    dve = DatabaseVectorsEngine(settings)
    await dve.database_to_vectors()
    
asyncio.run(run_transform())