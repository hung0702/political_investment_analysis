import psycopg2
import os
from dotenv import load_dotenv
from fetch.transactions import fetch_transactions as fetch_transactions
import config

load_dotenv()

def connect_db():
    try:
        connection = psycopg2.connect(
            host=os.getenv('HOST_IP_ADDRESS'),
            port=os.getenv('HOST_PORT'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        return connection
    except Exception as e:
        print("Error connecting to the database:", e)
        return None

def execute_sql_from_file(filename, connection):
    cursor = connection.cursor()
    with open(filename, 'r') as file:
        sql_script = file.read()
    cursor.execute(sql_script)
    connection.commit()
    cursor.close()
    print("SQL script executed successfully.")

ALLOWED_TABLES = ['senate_transactions', 'house_transactions']
ALLOWED_COLUMNS = {
    'senate_transactions': ['transaction_date', 'owner', 'ticker', 'asset_description', 'asset_type', 'type', 'amount', 'comment', 'party', 'state', 'industry', 'sector', 'senator', 'ptr_link', 'disclosure_date'],
    'house_transactions': ['transaction_date', 'owner', 'ticker', 'asset_description', 'type', 'amount', 'party', 'state', 'industry', 'sector', 'cap_gains_over_200_usd', 'representative', 'district', 'ptr_link', 'disclosure_date', 'disclosure_year']
}

def validate_table_and_columns(table_name, transaction_data):
    if table_name not in ALLOWED_TABLES:
        raise ValueError("Invalid table name")
    
    invalid_columns = [col for col in transaction_data.keys() if col not in ALLOWED_COLUMNS[table_name]]
    if invalid_columns:
        raise ValueError(f"Invalid column names in data: {', '.join(invalid_columns)}")

def insert_transactions(transactions, connection, table_name):
    if transactions:
        cursor = connection.cursor()
        try:
            transaction_data_sample = vars(transactions[0])
            validate_table_and_columns(table_name, transaction_data_sample)

            columns = ', '.join(transaction_data_sample.keys())
            value_placeholders = ', '.join(['%s'] * len(transaction_data_sample))
            sql_template = f"INSERT INTO {table_name} ({columns}) VALUES ({value_placeholders})"

            for transaction in transactions:
                values = list(vars(transaction).values())
                cursor.execute(sql_template, values)
            connection.commit()
        except Exception as e:
            print("Error inserting transaction:", e)
            connection.rollback()
        finally:
            cursor.close()

if __name__ == "__main__":
    conn = connect_db()
    # print("Host IP:", os.getenv('HOST_IP_ADDRESS'))
    # print("Port:", os.getenv('HOST_PORT'))
    # print("DB Name:", os.getenv('DB_NAME'))
    # print("DB User:", os.getenv('DB_USER'))
    if conn:
        try:
            # Create clean transaction tables
            # TODO logic to upsert if transactions change (but they should not,
            # that's what disclosure admendments are for)
            execute_sql_from_file('db/create_clean_transaction_tables.sql', conn)

            # Fetch transactions
            senate_transactions = fetch_transactions(config.SENATE_CSV_URL)
            house_transactions = fetch_transactions(config.HOUSE_CSV_URL)
            
            # Insert transactions into the database
            insert_transactions(senate_transactions, conn, 'senate_transactions')
            insert_transactions(house_transactions, conn, 'house_transactions')
        finally:
            # Close the database connection
            conn.close()
