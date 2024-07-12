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

def insert_transactions(transactions, connection, table_name):
    cursor = connection.cursor()
    try:
        for transaction in transactions:
            placeholders = ', '.join(['%s'] * len(vars(transaction)))
            columns = ', '.join(vars(transaction).keys())
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            values = list(vars(transaction).values())
            cursor.execute(sql, values)
        connection.commit()
    except Exception as e:
        print("Error inserting transaction:", e)
        connection.rollback()
    finally:
        cursor.close()

if __name__ == "__main__":
    conn = connect_db()
    print("Host IP:", os.getenv('HOST_IP_ADDRESS'))
    print("Port:", os.getenv('HOST_PORT'))
    print("DB Name:", os.getenv('DB_NAME'))
    print("DB User:", os.getenv('DB_USER'))
    if conn:
        try:
            # Fetch transactions
            senate_transactions = fetch_transactions(config.SENATE_CSV_URL)
            house_transactions = fetch_transactions(config.HOUSE_CSV_URL)
            
            # Insert transactions into the database
            insert_transactions(senate_transactions, conn, 'senate_transactions')
            insert_transactions(house_transactions, conn, 'house_transactions')
        finally:
            # Close the database connection
            conn.close()
