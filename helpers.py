from abc import ABC, abstractmethod

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


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
                    # Use SQLAlchemy text to safely construct the SQL statement
                    placeholders = ", ".join([f":col{i}" for i in range(len(row))])
                    query = text(f"INSERT INTO {table_name} VALUES ({placeholders})")
                    params = {f"col{i}": value for i, value in enumerate(row)}
                    conn.execute(query, **params)
                conn.commit()
            print("Data posted to SQL Server successfully.")
        except SQLAlchemyError as e:
            print(f"Error posting data to SQL Server: {e}")

    def execute(self, pg_query, sqlserver_table, options:dict={}):
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
    def transform_data(self, data, options:dict={}):
        """
        Example transformation: Convert all text to uppercase.
        """
        transformed_data = [
            tuple(str(item).upper() if isinstance(item, str) else item for item in row)
            for row in data
        ]
        print(transformed_data)
        return transformed_data
