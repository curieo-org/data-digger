from sqlalchemy import (
    create_engine,
    text
)
from typing import (
    List,
    Tuple
)
import json
from concurrent.futures import ThreadPoolExecutor
import asyncio
from app.config import (
    PG_POOL_SIZE,
    PG_MAX_OVERFLOW
)

class PGEngine():
    def __init__(self, database_url) -> None:
        self.engine = create_engine(database_url, pool_size=PG_POOL_SIZE, max_overflow=PG_MAX_OVERFLOW)
    
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

def transfer_table_data(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine,
    table_name: str,
    columns: List[str]
) -> None:
    select_query = 'SELECT ' + ', '.join(columns) + ' FROM ' + table_name + ' ORDER BY ' + columns[0] + ' ASC LIMIT {batch_size} OFFSET {offset};'

    insert_query = 'INSERT INTO ' + table_name + ' (' + ', '.join(columns) + ') VALUES (' + ', '.join([f':{column}' for column in columns]) + ') ON CONFLICT (' + columns[0] + ') DO UPDATE SET ' + ', '.join([f'{column} = EXCLUDED.{column}' for column in columns[1:]]) + ';'

    count_query = f'SELECT count(*) FROM {table_name}'

    row_count = local_pg_engine.execute_query(count_query)[0][0]
    print(f'Number of rows in the local {table_name}: {row_count}')
    
    batch_size = 100

    with ThreadPoolExecutor(max_workers=10) as executor:
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
                columns,
                batch_size,
                offset
            ))

        asyncio.gather(*tasks)

    result = prod_pg_engine.execute_query(count_query)
    print(f'Number of rows in the production {table_name}: {result[0][0]}')

    if result[0][0] == row_count:
        print(f'Data transfer completed for {table_name}')
    else:
        print(f'Data transfer partially completed for {table_name}')