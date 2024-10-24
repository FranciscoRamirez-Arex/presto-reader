import os

import pandas as pd
import prestodb
import requests 
from dotenv import load_dotenv

load_dotenv('.env')

PRESTO_HOST = os.getenv('PRESTO_HOST')
PRESTO_PORT = os.getenv('PRESTO_PORT')
PRESTO_USERNAME = os.getenv('PRESTO_USERNAME')
PRESTO_PASSWORD = os.getenv('PRESTO_PASSWORD')

print(f'presto host: {PRESTO_HOST}')

conn = prestodb.dbapi.connect(
    host=PRESTO_HOST,
    port=PRESTO_PORT,
    user=PRESTO_USERNAME,
    http_scheme='https',
    auth=prestodb.auth.BasicAuthentication(PRESTO_USERNAME, PRESTO_PASSWORD)
)

conn._http_session.verify = False


# Extract data from source
query = "SELECT * FROM sales.public.df_comp_test101 LIMIT 10"
df = pd.read_sql(query, conn).to_dict(orient='records')

# Transform data
payload = []

for record in df:
    payload_record = {}

    payload_record['journal_transaction_id'] = record['oracle_bank_transactions_general_ledger_transaction_id']
    payload_record['transaction_amount'] = record['oracle_bank_transactions_general_ledger_amt']
    payload_record['audit_control'] = 1 if record['match_status'] == 'MATCH' else 0

    payload.append(payload_record)

print(payload)

# Load data to API endpoint
url = 'http://localhost:8000/journal-records/'

response = requests.post(url, json=payload)

## print response status and content
print(f"Status Code: {response.status_code}")
print(f"Response content: {response.json()}")
