from abc import ABC, abstractmethod
import logging

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


# Setup logging
logging.basicConfig(
    filename="etl_process.log", level=logging.ERROR, format="%(asctime)s %(message)s"
)


def get_journalRecords(conn, query):
    # Extract data
    df: pd.DataFrame = pd.DataFrame(pd.read_sql(query, conn).to_dict(orient="records"))

    # TODO map columns (get the mapping dynamically)
    api_columns = {
        "oracle_bank_transactions_general_ledger_transaction_id": "journal_transaction_id",
        "oracle_bank_transactions_general_ledger_amt": "transaction_amount",
        "match_status": "audit_control",
    }

    # TODO: add try except
    # Transform
    journal_records = df[api_columns.keys()].copy(deep=True)
    journal_records.rename(columns=api_columns, errors="raise", inplace=True)
    journal_records["audit_control"] = journal_records["audit_control"].apply(
        lambda x: True if x == "MATCH" else False
    )

    return journal_records.to_dict(orient="records")


# TODO: convert args into a config dict
class APIPostAction(ABC):
    def __init__(
        self,
        query: str,
        api_columns: dict,
        dest_endpoint: str,
        headers: dict | None = None,
        conn=None,
        source_endpoint=None,
    ) -> None:
        self.conn = conn
        self.query = query
        self.api_columns: dict = api_columns
        self.headers = headers | {"Content-Type": "application/json"}
        self.dest_endpoint = dest_endpoint
        self.data = None
        self.df = None
        self.source_endpoint = source_endpoint

    def get_data(self):
        try:
            response = requests.get(self.source_endpoint)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx and 5xx)

            # Process the successful response
            self.data = response.json()  # Assuming JSON response
            self.df = pd.DataFrame(self.data)
            print("Request succeeded:", self.data)

        except requests.exceptions.HTTPError as http_err:
            print(
                f"HTTP error occurred: {http_err}"
            )  # HTTP error response, like 404 or 500
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred: {conn_err}")  # Network-related error
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout error occurred: {timeout_err}")  # Request timed out
        except requests.exceptions.RequestException as req_err:
            print(f"An error occurred: {req_err}")  # General error
        except ValueError as json_err:
            print(f"JSON decoding error: {json_err}")  # Error in decoding JSON response
        else:
            print("Request completed successfully.")
        finally:
            print("Request process ended.")

    @abstractmethod
    def transform_data(self):
        """
        Transform and shape the data
        """
        pass

    def post_data(self):
        try:
            response = requests.post(
                self.dest_endpoint, data=self.transform_data(), headers=self.headers
            )
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx and 5xx)

            # Process the successful response
            data = response.json()  # Assuming JSON response
            print("Request succeeded:", data)
            return data

        except requests.exceptions.HTTPError as http_err:
            print(
                f"HTTP error occurred: {http_err}"
            )  # HTTP error response, like 404 or 500
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred: {conn_err}")  # Network-related error
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout error occurred: {timeout_err}")  # Request timed out
        except requests.exceptions.RequestException as req_err:
            print(f"An error occurred: {req_err}")  # General error
        except ValueError as json_err:
            print(f"JSON decoding error: {json_err}")  # Error in decoding JSON response
        else:
            print("Request completed successfully.")
        finally:
            print("Request process ended.")


class POSTAction(ABC):
    def __init__(self, pg_conn_string, sqlserver_conn_string):
        # TODO: rename conn strings and engines as source and destination
        """
        Initialize PostgreSQL and SQL Server connections using SQLAlchemy connection strings.

        :param pg_conn_string: SQLAlchemy connection string for PostgreSQL.
        :param sqlserver_conn_string: SQLAlchemy connection string for SQL Server.
        """
        self.pg_engine = create_engine(pg_conn_string)
        self.sqlserver_engine = create_engine(sqlserver_conn_string)

    def read_data_from_postgres(self, query):
        """
        Connects to PostgreSQL and reads data based on a provided query.
        """
        try:
            with self.pg_engine.connect() as conn:
                result = conn.execute(text(query))
                data = result.fetchall()
            return data
        except SQLAlchemyError as e:
            print(f"Error reading data from PostgreSQL: {e}")
            return None

    @abstractmethod
    def transform_data(self, data):
        """
        Abstract method for transforming data.
        This needs to be implemented by subclasses and return the transformed data.
        """
        pass

    def post_data_to_sqlserver(self, table_name, data):
        """
        Connects to SQL Server and inserts transformed data into a specified table.
        """
        try:
            with self.sqlserver_engine.connect() as conn:
                for row in data:
                    # Generate a query with positional parameters
                    placeholders = ", ".join(["?" for _ in row])
                    query = f"INSERT INTO {table_name} VALUES ({placeholders})"

                    # Execute the query with the row as a tuple
                    print("row:", row)
                    conn.execute(text(query), tuple(row))
                conn.commit()
            print("Data posted to SQL Server successfully.")
        except SQLAlchemyError as e:
            print(f"Error posting data to SQL Server: {e}")

    def execute(self, pg_query, sqlserver_table, options: dict = {}):
        """
        Orchestrates reading from PostgreSQL, transforming the data,
        and posting to SQL Server.
        """
        data = self.read_data_from_postgres(pg_query)
        if data is None:
            print("No data to process.")
            return
        transformed_data = self.transform_data(data, options)
        self.post_data_to_sqlserver(sqlserver_table, transformed_data)


class MyDataTransformer(POSTAction):
    def transform_data(self, data, options: dict = {}):
        """
        Example transformation: Convert all text to uppercase.
        """
        transformed_data = [
            tuple(str(item).upper() if isinstance(item, str) else item for item in row)
            for row in data
        ]
        print("Transformed data: ", transformed_data)
        return transformed_data


"""
ETL Pandas ABC class
"""


class ETLProcess(ABC):
    def __init__(self, source_engine, destination_engine):
        self.source_engine = source_engine
        self.destination_engine = destination_engine

    def run(self, extract_query, destination_table, options=None):
        """
        Run the ETL process with a specified extract query, destination table, and options for transformation.
        """
        options = (
            options or {}
        )  # Default to an empty dictionary if no options are provided
        try:
            data = self.extract(extract_query)
            transformed_data = self.transform(data, options)
            self.load(transformed_data, destination_table)
            print("ETL process completed successfully.")

        except Exception as e:
            logging.error(f"Error during ETL process: {e}")
            print("ETL process failed.")

    def extract(self, extract_query):
        """Extract data from the source using the provided query."""
        try:
            data = pd.read_sql(extract_query, self.source_engine)
            print("Data extracted successfully.")
            return data
        except Exception as e:
            logging.error(f"Error during data extraction: {e}")
            raise

    @abstractmethod
    def transform(self, data, options):
        """Transform the data. This method must be implemented by subclasses."""
        pass

    def load(self, transformed_data, destination_table):
        """Load the transformed data into the specified destination table."""
        try:
            transformed_data.to_sql(
                destination_table,
                self.destination_engine,
                if_exists="append",
                index=False,
            )
            print(f"Data loaded successfully into the table: {destination_table}.")
        except Exception as e:
            logging.error(f"Error during data loading: {e}")
            raise


# Example subclass implementing the transform method
class CustomETL(ETLProcess):
    def transform(self, data, options):
        """Apply transformations to the extracted data based on options."""
        try:
            # Example of using options to customize transformations
            if "rename_columns" in options:
                for old_name, new_name in options["rename_columns"].items():
                    data.rename(columns={old_name: new_name}, inplace=True)

            if "insert_columns" in options:
                for column, value in options["insert_columns"].items():
                    data[column] = value

            transformed_data = data
            print("Data transformed successfully.")
            return transformed_data
        except Exception as e:
            logging.error(f"Error during data transformation: {e}")
            raise
