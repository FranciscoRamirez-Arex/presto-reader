from abc import ABC, abstractmethod
import requests

import pandas as pd


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


class PokeAPI(APIPostAction):
    def transform_data(self):
        return "ok"
