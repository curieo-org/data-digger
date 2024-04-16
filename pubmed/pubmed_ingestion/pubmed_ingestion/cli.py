import click
import os
from dotenv import load_dotenv

from pubmed_ingestion.database_vectordb_transform import DatabaseVectorsEngine

load_dotenv()
database_engine = os.getenv("POSTGRES_ENGINE")

@click.command()
@click.option('--qdrant_url_address', type=str, default='localhost', help='provide Qdrant URL address')
@click.option('--qdrant_url_port', type=int, default=6333, help='provide Qdrant URL port address')
@click.option('--qdrant_collection_name', type=str, default="pubmed_hybrid_vector_db", help='provide Qdrant Pubmed Collection Address')
@click.option('--qdrant_api_key', type=str, default="z0U3Kdzu", help='provide Qdrant API Key')
def transform_database_to_vectors(
    qdrant_url_address:str,
    qdrant_url_port:int,
    qdrant_collection_name: str,
    qdrant_api_key:str
    ):

    dve = DatabaseVectorsEngine(
        qdrant_url_address=qdrant_url_address,
        qdrant_url_port=qdrant_url_port,
        qdrant_collection_name=qdrant_collection_name,
        qdrant_api_key=qdrant_api_key
        )
    
    dve.database_to_vectors(database_engine=database_engine)

if __name__ == '__main__':
    transform_database_to_vectors()