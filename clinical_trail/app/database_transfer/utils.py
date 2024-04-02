from sqlalchemy import create_engine, text
from typing import List, Tuple
import json


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