# main.py
import os

from dotenv import load_dotenv
import pandas as pd
from pyhive import presto

load_dotenv()

# Establich connection to Presto
conn = presto.connect(
    host=os.getenv('PRESTO_HOST'),
    port=os.getenv('PRESTO_PORT'),
    username=os.getenv('PRESTO_USERNAME'),
    catalog=os.getenv('PRESTO_CATALOG'),
    schema=os.getenv('PRESTO_SCHEMA')
)

# Run a SQL query
query = "SELECT * FROM table LIMIT 10"
df = pd.read_sql(query, conn)

# Display the fetched data
print(df)
