from typing import List, Tuple

from sqlalchemy import create_engine, text

from app.settings import CTDatabaseReaderSettings


class PGEngine():
    def __init__(self, database_url, database_reader: CTDatabaseReaderSettings) -> None:
        self.engine = create_engine(
            database_url,
            pool_size=database_reader.pool_size, 
            max_overflow=database_reader.max_overflow
        )
    
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

class TableStructure:
    def __init__(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        embeddable_columns: List[str]
    ) -> None:
        self.table_name = table_name
        self.columns = columns
        self.primary_keys = primary_keys
        self.embeddable_columns = embeddable_columns