# main.py
import requests
from config import conn, pg_conn_string, sqlserver_conn_string
from helpers import get_journalRecords, MyDataTransformer
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError



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

print(pg_conn_string)
print('****')
print(sqlserver_conn_string)

pg_engine = create_engine(pg_conn_string)
sqlserver_engine = create_engine(sqlserver_conn_string)
query = 'SELECT name, price FROM items'
mssql_query = 'SELECT ProductName, Price FROM Products'

try:
    with pg_engine.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            print(row)
except SQLAlchemyError as e:
    print(f"Error reading data from PostgreSQL: {e}")

print('****')

transformer = MyDataTransformer(pg_conn_string,sqlserver_conn_string )
transformer.execute(
    pg_query="SELECT * FROM items",
    sqlserver_table='Products'
)

print('****')
try:
    with sqlserver_engine.connect() as conn:
        result = conn.execute(text(mssql_query))
        for row in result:
            print(row)
except SQLAlchemyError as e:
    print(f"Error reading data from SQLServer: {e}")
