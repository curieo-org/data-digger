from concurrent.futures import ThreadPoolExecutor
import asyncio
from .utils import PGEngine, transfer_data_batch

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