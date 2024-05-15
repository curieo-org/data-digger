from typing import Any, Dict, Tuple
from sqlalchemy import text, insert, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError

def run_select_sql(engine, command: str) -> Dict:
        """Execute a SQL statement and return a string representing the results.

        If the statement returns rows, a string of the results is returned.
        If the statement returns no rows, an empty string is returned.
        """
        with engine.begin() as connection:
            try:
                cursor = connection.execute(text(command))
            except (SQLAlchemyError, ValueError) as exc:
                raise Exception("Failed to fetch records from the database.") from exc
            if cursor.returns_rows:
                result = cursor.fetchall()
                results = []
                for row in result:
                    results.append(row)
                return {
                    "result": results,
                    "col_keys": list(cursor.keys()),
                }
        return {}

def run_insert_sql(engine, table_name, data_dict):
    """
    Insert a new row into the specified table in the database.

    Parameters:
    - engine: The SQLAlchemy engine to use for the database connection.
    - table_name: The name of the table to insert the data into.
    - data_dict: A dictionary containing the data to be inserted into the table.

    Returns:
    None

    Raises:
    - SQLAlchemyError: If an error occurs while executing the insert statement.
    - ValueError: If the data dictionary is empty or contains invalid data.

    This function uses SQLAlchemy's autoload_with feature to dynamically load the table metadata from the database. It then begins a new transaction and attempts to insert the provided data into the specified table. If the insertion is successful, the transaction is committed. If an error occurs during the insertion, the transaction is rolled back and the exception is re-raised.
    """
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    with engine.begin() as connection:
        try:
            stmt = insert(table)
            connection.execute(stmt, data_dict)
            return True
        except (SQLAlchemyError, ValueError) as exc:
            raise Exception("Failed to fetch records from the database.") from exc
            return False
            