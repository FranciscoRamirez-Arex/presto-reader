import pandas as pd

def get_journalRecords(conn, query):
    # Extract data
    df: pd.DataFrame = pd.DataFrame(pd.read_sql(query, conn).to_dict(orient='records'))

    api_columns = {
        'oracle_bank_transactions_general_ledger_transaction_id': 'journal_transaction_id',
        'oracle_bank_transactions_general_ledger_amt': 'transaction_amount',
        'match_status': 'audit_control',
    }

    # Transform
    journal_records = df[api_columns.keys()].copy(deep=True)
    journal_records.rename(columns=api_columns, errors="raise", inplace=True)
    journal_records['audit_control'] = journal_records['audit_control'].apply(lambda x: True if x == 'MATCH' else False)

    return journal_records.to_dict(orient='records')
