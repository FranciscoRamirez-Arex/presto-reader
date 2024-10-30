# main.py
import requests
from config import conn, pg_conn_string, sqlserver_conn_string
from helpers import CustomETL
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


print(pg_conn_string)
print(sqlserver_conn_string)
print('****')
print('ETL data extraction')
print('****')

pg_engine = create_engine(pg_conn_string)
sqlserver_engine = create_engine(sqlserver_conn_string)
query = 'SELECT name, price FROM items'
mssql_query = 'SELECT * FROM Products2'

try:
    with pg_engine.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            print(row)
except SQLAlchemyError as e:
    print(f"Error reading data from PostgreSQL: {e}")

print('****')
print('ETL class transform')
print('****')

# Create an instance of the CustomETL class
etl_process = CustomETL(source_engine=pg_engine, destination_engine=sqlserver_engine)

# Define the extract query, destination table, and transformation options
extract_query = query
destination_table = "Products2"
options = {
    'rename_columns': {
        'name': 'ProductName',
        'price': 'Price'
        },
    'insert_columns': {'Category':'ETL inserts'  } # Example of column insertion
}

# Run the ETL process with the specified extract query, destination table, and options
etl_process.run(extract_query, destination_table, options)

print('****')
print('ETL load results')
print('****')
try:
    with sqlserver_engine.connect() as conn:
        result = conn.execute(text(mssql_query))
        for row in result:
            print(row)
except SQLAlchemyError as e:
    print(f"Error reading data from SQLServer: {e}")
