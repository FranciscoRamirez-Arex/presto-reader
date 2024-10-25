import pandas as pd

def get_dataframe(conn, query):
    return pd.read_sql(query, conn).to_dict(orient='records')
    