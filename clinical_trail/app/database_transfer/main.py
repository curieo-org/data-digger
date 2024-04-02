from .utils import PGEngine
from .tbl_studies_info import transfer_tbl_studies_info
from app.config import PG_LOCAL_DATABASE, PG_PROD_DATABASE, PG_PROD_SETUP_FILE

async def transfer_all_tables() -> None:
    local_pg_engine = PGEngine(PG_LOCAL_DATABASE)

    prod_pg_engine = PGEngine(PG_PROD_DATABASE)

    # Setup the prod database if it is not already
    with open(PG_PROD_SETUP_FILE, 'r') as file:
        queries = file.read()

        prod_pg_engine.execute_query_without_return(queries)

    # Transfer data for all tables
    transfer_tbl_studies_info(local_pg_engine, prod_pg_engine)
    print('Data transfer completed for all tables')