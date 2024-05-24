from typing import Generator, List

from app.database_transfer import PGEngine, TableStructure


class DatabaseReader:
    def __init__(
        self,
        database_engine: PGEngine
    ):
        self.database_engine = database_engine

    def load_data_in_batches(
        self,
        table_structure: TableStructure,
        batch_size: int = 100,
    ) -> Generator[List[dict], None, None]:
        """Custom query and load data method.
        
        This overridden version might perform additional operations,
        such as filtering results based on custom criteria or enriching
        the documents with additional information.
        
        Args:
            query (str): Query parameter to filter tables and rows.
        
        Returns:
            List[Document]: A list of custom Document objects.
        """
        table_structure = table_structure
        columns = table_structure.embeddable_columns + table_structure.vector_metadata_columns + table_structure.primary_keys
        columns = list(set(columns))
        query_template = f"""
            SELECT {', '.join(columns)} 
            FROM {table_structure.table_name} 
            ORDER BY {table_structure.primary_keys[0]} ASC 
            LIMIT {batch_size} OFFSET {{offset}};
        """

        offset = 0

        while True:
            query = query_template.format(offset=offset)
            if query is None:
                raise ValueError("A query parameter is necessary to filter the data")

            data_rows: List[str] = []
            result = self.database_engine.execute_query(query)

            for item in result:
                row_object = dict(zip(columns, item))
                data_rows.append(row_object)

            yield data_rows
            offset += batch_size
