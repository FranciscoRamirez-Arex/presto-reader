# config.py
import os

from dotenv import load_dotenv
import prestodb

load_dotenv('.env')

PRESTO_HOST = os.getenv('PRESTO_HOST')
PRESTO_PORT = os.getenv('PRESTO_PORT')
PRESTO_USERNAME = os.getenv('PRESTO_USERNAME')
PRESTO_PASSWORD = os.getenv('PRESTO_PASSWORD')

conn = prestodb.dbapi.connect(
    host=PRESTO_HOST,
    port=PRESTO_PORT,
    user=PRESTO_USERNAME,
    http_scheme='https',
    auth=prestodb.auth.BasicAuthentication(PRESTO_USERNAME, PRESTO_PASSWORD)
)

conn._http_session.verify = False

pg_conn_string = "postgresql+psycopg2://admin:Sandbox_2023@localhost:5432/fastapi"
sqlserver_conn_string = "mssql+pyodbc://sa:YourStrongPassword123!@localhost:1433/contoso?driver=ODBC+Driver+17+for+SQL+Server"
