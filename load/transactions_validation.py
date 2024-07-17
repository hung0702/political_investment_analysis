ALLOWED_TABLES = ['senate_transactions', 'house_transactions']
ALLOWED_COLUMNS = {
    'senate_transactions': ['transaction_date', 'owner', 'ticker', 'clean_ticker', 'crypto', 'asset_description', 'asset_type', 'type', 'amount', 'comment', 'party', 'state', 'industry', 'sector', 'senator', 'ptr_link', 'disclosure_date'],
    'house_transactions': ['transaction_date', 'owner', 'ticker', 'clean_ticker', 'crypto', 'asset_description', 'type', 'amount', 'party', 'state', 'industry', 'sector', 'cap_gains_over_200_usd', 'representative', 'district', 'ptr_link', 'disclosure_date', 'disclosure_year']
}

def validate_table_and_columns(table_name, transaction_data):
    if table_name not in ALLOWED_TABLES:
        raise ValueError("Invalid table name")
    
    invalid_columns = [col for col in transaction_data.keys() if col not in ALLOWED_COLUMNS[table_name]]
    if invalid_columns:
        raise ValueError(f"Invalid column names in data: {', '.join(invalid_columns)}")

def validate_transaction_data(transactions, table_name):
    for transaction in transactions:
        validate_table_and_columns(table_name, vars(transaction))
