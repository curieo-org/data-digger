
from typing import List
from llama_index.core.schema import (
    Document
)
from llama_index.readers.database import DatabaseReader
from sqlalchemy import text


class PubmedDatabaseReader(DatabaseReader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.offset = 0  # Initialize offset

    def load_data_in_batches(
            self, 
            query_template: str, 
            batch_size: int = 100
            ) -> List[Document]: 
        """Custom query and load data method.
        
        This overridden version might perform additional operations,
        such as filtering results based on custom criteria or enriching
        the documents with additional information.
        
        Args:
            query (str): Query parameter to filter tables and rows.
        
        Returns:
            List[Document]: A list of custom Document objects.
        """
        offset = 0
        while True:
            query = query_template.format(batch_size=batch_size, offset=offset)

            documents = []
            with self.sql_database.engine.connect() as connection:
                if query is None:
                    raise ValueError("A query parameter is necessary to filter the data")
                else:
                    result = connection.execute(text(query))

                for item in result.fetchall():
                    # Custom processing of each item
                    # Example: Append a suffix to each column value
                    pubid = item[0]
                    doc_str = ",".join([abstract['string'] for abstract in item[1]])
                    title = ",".join([title['string'] for title in item[2]])
                    pubdate = item[3]
                    year = item[4]
                    
                    documents.append(Document(text=doc_str, metadata={"title" : title, "pubid" : pubid, "year": year, "pubdate": pubdate}))
            yield documents
            offset += batch_size