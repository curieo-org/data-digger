from os import environ

from dotenv import load_dotenv

load_dotenv()

PG_LOCAL_DATABASE = environ.get('PG_LOCAL_DATABASE', 'postgresql://postgres@localhost:5430/aact')
PG_PROD_DATABASE = environ.get('PG_PROD_DATABASE', 'postgresql://postgres@localhost:5431/aact')

PG_POOL_SIZE = int(environ.get('PG_POOL_SIZE', 10))
PG_MAX_OVERFLOW = int(environ.get('PG_MAX_OVERFLOW', 0))
PG_PROD_SETUP_FILE = 'assets/prod_setup.sql'

QDRANT_HOST = environ.get('QDRANT_HOST', 'localhost')
QDRANT_PORT = int(environ.get('QDRANT_PORT', 6333))
QDRANT_COLLECTION_NAME = environ.get('QDRANT_COLLECTION_NAME', 'clinical_trials_vector_db')
QDRANT_API_KEY = environ.get('QDRANT_API_KEY', 'z0U3Kdzu')