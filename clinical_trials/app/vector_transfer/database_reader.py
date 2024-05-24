from enum import Enum
from typing import Generator, List

from loguru import logger

from app.database_transfer import PGEngine, TableStructure

logger.add(
    "file.log",
    rotation="500 MB",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
)


class VectorIngestionStatus(Enum):
    PENDING = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    UNEXPECTED_ERROR = 3
    FAILED = 4


class DatabaseReader:
    def __init__(self, database_engine: PGEngine):
        self.database_engine = database_engine
        self.ingestion_status_table_name = "vector_ingestion_log"

    def create_ingestion_status_table(self, table_structure: TableStructure):
        """Create a log status table to keep track of the transfer status."""

        primary_keys = ", ".join(
            [f"{key} varchar(255) NOT NULL" for key in table_structure.primary_keys]
        )

        query = f"""
            CREATE TABLE IF NOT EXISTS {self.ingestion_status_table_name} (
                {primary_keys},
                status integer NOT NULL,
                embeddable_data jsonb,
                metadata jsonb,
                updated_at timestamp DEFAULT NOW(),

                PRIMARY KEY ({", ".join(table_structure.primary_keys)})
            );
        """

        self.database_engine.execute_query_without_return(query)
        logger.info("Log status table created")

    def prepare_ingestion_status(
        self,
        table_structure: TableStructure,
    ):
        """Prepare the ingestion status table for the transfer process."""

        self.create_ingestion_status_table(table_structure)

        embeddable_json = ", ".join(
            [f"'{column}', {column}" for column in table_structure.embeddable_columns]
        )
        metadata_json = ", ".join(
            [
                f"'{column}', {column}"
                for column in table_structure.vector_metadata_columns
            ]
        )

        query = f"""
            INSERT INTO vector_ingestion_log ({", ".join(table_structure.primary_keys)}, status, embeddable_data, metadata, updated_at)
            SELECT {", ".join(table_structure.primary_keys)}, {VectorIngestionStatus.PENDING.value},
            jsonb_build_object({embeddable_json}) as embeddable_data,
            jsonb_build_object({metadata_json}) as metadata,
            NOW() as updated_at
            FROM {table_structure.table_name}
            ON CONFLICT ({", ".join(table_structure.primary_keys)}) DO UPDATE SET embeddable_data = EXCLUDED.embeddable_data, metadata = EXCLUDED.metadata, status = EXCLUDED.status, updated_at = EXCLUDED.updated_at;
        """

        self.database_engine.execute_query_without_return(query)
        logger.info("Log status table prepared")

    def update_ingestion_status(
        self,
        primary_keys: dict[str, str],
        status: VectorIngestionStatus,
    ):
        """Update the ingestion status table with the new status."""

        query = f"""
            UPDATE vector_ingestion_log
            SET status = {status.value}
            WHERE {", ".join([f"{key} = '{value}'" for key, value in primary_keys.items()])};
        """

        self.database_engine.execute_query_without_return(query)
        logger.info("Log status table updated")

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
            table_structure (TableStructure): The table structure of the data.
            batch_size (int, optional): The number of rows to load in each batch. Defaults to 100.

        Returns:
            List[Document]: A list of objects representing the data.
        """

        columns = table_structure.primary_keys + [
            "status",
            "embeddable_data",
            "metadata",
        ]
        query_template = f"""
            SELECT {', '.join(columns)} FROM {self.ingestion_status_table_name} 
            WHERE status != {VectorIngestionStatus.COMPLETED.value} 
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
