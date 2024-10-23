import prestodb
import os

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

# conn._http_session.verify = False

# cur = conn.cursor()
# cur.execute('SELECT * FROM sales.public.df_comp_test101 LIMIT 10')
# rows = cur.fetchall()

# print(rows)
