import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import List

from app.database_transfer.utils import PGEngine, TableStructure
from app.settings import CTDatabaseReaderSettings


def transfer_data_batch(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    select_query: str,
    insert_query: str,
    columns: List[str],
    batch_size: int,
    offset: int
) -> None:
    select_query = select_query.format(batch_size=batch_size, offset=offset)
    rows = source_pg_engine.execute_query(select_query)
    params = []

    for row in rows:
        params.append({field: json.dumps(row[i]) for i, field in enumerate(columns)})

    target_pg_engine.execute_query_with_params(insert_query, params)

    print('Batch transfer completed: offset:', offset, 'batch_size:', batch_size)

def transfer_table_data(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    table_structure: TableStructure,
    database_reader: CTDatabaseReaderSettings,
) -> None:
    table_name = table_structure.table_name
    columns = table_structure.columns
    primary_keys = table_structure.primary_keys

    select_query = 'SELECT ' + ', '.join(columns) + ' FROM ' + table_name + ' ORDER BY ' + table_structure.primary_keys[0] + ' ASC LIMIT {batch_size} OFFSET {offset};'

    insert_query = 'INSERT INTO ' + table_name + ' (' + ', '.join(columns) + ') VALUES (' + ', '.join([f':{column}' for column in columns]) + ') ON CONFLICT (' + ', '.join([f'{column}' for column in primary_keys]) + ') DO UPDATE SET ' + ', '.join([f'{column} = EXCLUDED.{column}' for column in columns[1:]]) + ';'

    count_query = f'SELECT count(*) FROM {table_name}'

    try:
        row_count = source_pg_engine.execute_query(count_query)[0][0]
    except:
        raise Exception(
            f'Error in getting the row count for the table: {table_name}'
        )
    print(f'Number of rows in the local {table_name}: {row_count}')

    batch_size = database_reader.batch_size
    
    with ThreadPoolExecutor(max_workers=database_reader.max_workers) as executor:
        event_loop = asyncio.get_event_loop()
        tasks = []

        for offset in range(0, row_count, batch_size):
            tasks.append(event_loop.run_in_executor(
                executor,
                transfer_data_batch,
                source_pg_engine,
                target_pg_engine,
                select_query,
                insert_query,
                columns,
                batch_size,
                offset
            ))

        asyncio.gather(*tasks)

    try:
        result = target_pg_engine.execute_query(count_query)[0][0]
    except:
        raise Exception(
            f'Error in getting the row count for the table: {table_name}'
        )
    print(f'Number of rows in the production {table_name}: {result}')

    if result == row_count:
        print(f'Data transfer completed for {table_name}')
    else:
        print(f'Data transfer partially completed for {table_name}')