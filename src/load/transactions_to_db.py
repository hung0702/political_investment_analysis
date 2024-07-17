'''This script pulls CSV data from House Stock Watcher and Senate Stock Watcher
to build the inital list for transformation. Dates and tickers are transformed,
then database transactions are validated prior to insertion. Old tables are
dropped, and new tables are created.
'''

import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
from ..extract.transactions import fetch_transactions
from ..transform.date import convert_date
from ..transform.ticker import clean_ticker, is_cryptocurrency
from .transactions_validation import validate_transaction_data, ALLOWED_TABLES, ALLOWED_COLUMNS

import src.config as config


load_dotenv()


def connect_db():
    try:
        connection = psycopg2.connect(os.getenv('DB_CONFIG'))
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
    if table_name not in ALLOWED_TABLES:
        raise ValueError("Invalid table name")

    cursor = connection.cursor()
    inserted_count = 0

    try:
        for transaction in transactions:
            transformed_data = transform_transaction_data(vars(transaction))
            validate_transaction_data([transaction], table_name)  # Confirm data meets requirements

            columns = list(transformed_data.keys())
            for col in columns:
                if col not in ALLOWED_COLUMNS[table_name]:
                    raise ValueError(f"Invalid column: {col}")

            placeholders = ','.join(['%s'] * len(columns))
            
            # Constructing the SQL statement securely
            sql = psycopg2.sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
                table=psycopg2.sql.Identifier(table_name),
                fields=psycopg2.sql.SQL(', ').join(map(psycopg2.sql.Identifier, columns)),
                values=psycopg2.sql.SQL(placeholders)
            )

            values = list(transformed_data.values())
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


def main_function():
    conn = connect_db()
    
    if conn:
        drop_and_create_tables('src/load/create_clean_transaction_tables.sql', conn)

        senate_transactions = fetch_transactions(config.SENATE_CSV_URL)
        house_transactions = fetch_transactions(config.HOUSE_CSV_URL)
        
        insert_transactions(senate_transactions, conn, 'senate_transactions')
        insert_transactions(house_transactions, conn, 'house_transactions')

        conn.close()
