from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple
import json
import asyncio

from .config import PG_LOCAL_DATABASE, PG_PROD_DATABASE, PG_PROD_SETUP_FILE


class PGEngine():
    def __init__(self, database_url) -> None:
        self.engine = create_engine(database_url, pool_size=10, max_overflow=0)

    
    def execute_query(self, query: str) -> List[Tuple]:
        with self.engine.connect() as connection:
            result = connection.execute(text(query))

        return result.fetchall()
    
    
    def execute_query_with_params(self, query: str, params: dict) -> List[Tuple]:
        with self.engine.connect() as connection:
            connection.execute(text(query), params)
            connection.commit()
    
    
    def execute_query_without_return(self, query: str) -> None:
        with self.engine.connect() as connection:
            connection.execute(text(query))
            connection.commit()
    

def transfer_data_batch(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine,
    select_query: str,
    insert_query: str,
    columns: List[str],
    batch_size: int,
    offset: int
) -> None:
    select_query = select_query.format(batch_size=batch_size, offset=offset)
    rows = local_pg_engine.execute_query(select_query)
    params = []

    for row in rows:
        params.append({field: json.dumps(row[i]) for i, field in enumerate(columns)})

    prod_pg_engine.execute_query_with_params(insert_query, params)

    print('Batch transfer completed: offset:', offset, 'batch_size:', batch_size)


def transfer_tbl_studies_info(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    query = 'SELECT count(*) FROM tbl_studies_info'
    row_count = local_pg_engine.execute_query(query)[0][0]
    print('Number of rows in the local tbl_studies_info:', row_count)
    
    batch_size = 100

    select_query = 'SELECT nct_id, title, description, study_details FROM tbl_studies_info ORDER BY nct_id ASC LIMIT {batch_size} OFFSET {offset}'

    insert_query = 'INSERT INTO tbl_studies_info (nct_id, title, description, study_details) VALUES (:nct_id, :title, :description, :study_details) ON CONFLICT (nct_id) DO UPDATE SET title = EXCLUDED.title, description = EXCLUDED.description, study_details = EXCLUDED.study_details;'

    with ThreadPoolExecutor(max_workers=20) as executor:
        event_loop = asyncio.get_event_loop()
        tasks = []

        for offset in range(0, row_count, batch_size):
            tasks.append(event_loop.run_in_executor(
                executor,
                transfer_data_batch,
                local_pg_engine,
                prod_pg_engine,
                select_query,
                insert_query,
                ['nct_id', 'title', 'description', 'study_details'],
                batch_size,
                offset
            ))

        asyncio.gather(*tasks)

    query = 'SELECT count(*) FROM tbl_studies_info'
    result = prod_pg_engine.execute_query(query)
    
    print('Number of rows in the production tbl_studies_info:', result[0][0])
    if result[0][0] == row_count:
        print('Data transfer completed for tbl_studies_info')
    else:
        print('Data transfer partially completed for tbl_studies_info')


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