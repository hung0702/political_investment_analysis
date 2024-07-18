import psycopg2
from psycopg2 import sql

"""These allowed tables/column names are an extra security measure to avoid
potential SQL injection
"""
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

def insert_transactions(transactions, connection, table_name):
    if table_name not in ALLOWED_TABLES:
        raise ValueError("Invalid table name")

    cursor = connection.cursor()
    inserted_count = 0

    try:
        for transaction in transactions:
            validate_transaction_data([transaction], table_name) 

            columns = list(vars(transaction).keys())
            for col in columns:
                if col not in ALLOWED_COLUMNS[table_name]:
                    raise ValueError(f"Invalid column: {col}")

            placeholders = ','.join(['%s'] * len(columns))
            
            sql = psycopg2.sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
                table=psycopg2.sql.Identifier(table_name),
                fields=psycopg2.sql.SQL(', ').join(map(psycopg2.sql.Identifier, columns)),
                values=psycopg2.sql.SQL(placeholders)
            )

            values = list(vars(transaction).values())
            cursor.execute(sql, values)
            inserted_count += 1

        connection.commit()
        print(f"Successfully inserted {inserted_count} transactions.")

    except Exception as e:
        print("Error inserting transaction:", e)
        connection.rollback()

    finally:
        cursor.close()

    return inserted_count