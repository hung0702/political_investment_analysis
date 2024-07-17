import psycopg2
import os
from dotenv import load_dotenv
from extract.transactions import fetch_transactions
from transform.date import convert_date
from transform.ticker import clean_ticker, is_cryptocurrency
from load.transactions_validation import validate_transaction_data
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


def drop_and_create_tables(filename, connection):
    cursor = connection.cursor()
    with open(filename, 'r') as file:
        sql_script = file.read()
    cursor.execute(sql_script)
    connection.commit()
    cursor.close()
    print("Old SQL transaction tables dropped. New tables created.")


def transform_transaction_data(transaction):
    transaction['transaction_date'] = convert_date(transaction['disclosure_date'], transaction['transaction_date'])
    transaction['disclosure_date'] = convert_date(transaction['disclosure_date'])
    transaction['clean_ticker'] = clean_ticker(transaction['ticker'], transaction['asset_description'])
    transaction['crypto'] = is_cryptocurrency(transaction['ticker'])
    return transaction


def insert_transactions(transactions, connection, table_name):
    cursor = connection.cursor()
    inserted_count = 0

    try:
        for transaction in transactions:
            transformed_data = transform_transaction_data(vars(transaction))
            validate_transaction_data([transaction], table_name)  # anti-injection measures

            columns = ', '.join(transformed_data.keys())
            value_placeholders = ', '.join(['%s'] * len(transformed_data))
            sql_template = f"INSERT INTO {table_name} ({columns}) VALUES ({value_placeholders})"

            values = list(transformed_data.values())
            cursor.execute(sql_template, values)

        connection.commit()

    except Exception as e:
        print("Error inserting transaction:", e)
        connection.rollback()

    finally:
        cursor.close()


if __name__ == "__main__":
    conn = connect_db()
    
    if conn:
        drop_and_create_tables('load/create_clean_transaction_tables.sql', conn)

        senate_transactions = fetch_transactions(config.SENATE_CSV_URL)
        house_transactions = fetch_transactions(config.HOUSE_CSV_URL)
        
        insert_transactions(senate_transactions, conn, 'senate_transactions')
        insert_transactions(house_transactions, conn, 'house_transactions')

        print(f"Senate transactions: {len(senate_transactions)} inserted")
        print(f"House transactions: {len(house_transactions)} inserted")

        conn.close()
