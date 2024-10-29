# main.py
import requests
from config import conn
from helpers import get_journalRecords, PokeAPI


# Extract data from source
# query = "SELECT * FROM sales.public.df_comp_test101 LIMIT 30"
# payload = get_journalRecords(conn=conn, query=query)

# print(payload)

# # Load data to API endpoint
# url = 'http://localhost:8000/journal-records/'

# response = requests.post(url, json=payload)

# ## print response status and content
# print(f"Status Code: {response.status_code}\n\n")
# print(f"Response content: {response.json()}")

ditto = PokeAPI(
    query="",
    api_columns={},
    dest_endpoint="",
    headers={},
    conn=None,
    source_endpoint="http://localhost:8000/items"

)

ditto.get_data()
print(ditto.data)
print(ditto.df)
print(ditto.transform_data())
