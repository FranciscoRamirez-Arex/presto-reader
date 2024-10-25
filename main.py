# main.py
import requests 
from config import conn
from helpers import get_dataframe


# Extract data from source
query = "SELECT * FROM sales.public.df_comp_test101 LIMIT 10"
df = get_dataframe(conn=conn, query=query)

# Transform data
payload = []

for record in df:
    payload_record = {}

    payload_record['journal_transaction_id'] = record['oracle_bank_transactions_general_ledger_transaction_id']
    payload_record['transaction_amount'] = record['oracle_bank_transactions_general_ledger_amt']
    payload_record['audit_control'] = 1 if record['match_status'] == 'MATCH' else 0

    payload.append(payload_record)

# Load data to API endpoint
url = 'http://localhost:8000/journal-records/'

response = requests.post(url, json=payload)

## print response status and content
print(f"Status Code: {response.status_code}")
print(f"Response content: {response.json()}")
