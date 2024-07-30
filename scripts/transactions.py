"""
This script extracts, transforms, and loads house and senate transaction data to
a SQL database

1. Connects to specified database (set in .env file)
2. Drops existing transaction tables and creates new ones from specified SQL scripts
3. Fetches new transaction data from predefined CSV sources (set in config.py).
4. Transforms the fetched data to match the database schema and to correct any inconsistencies
5. Inserts the transformed data into the database
6. Applies manual SQL corrections from specified SQL scripts
"""


import sys
import os

# Ensure the root directory is in sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from src.load.connection import connect_db, drop_tables, create_tables, apply_manual_corrections
from src.extract.get_transactions import fetch_transactions
from src.transform.transform_transactions import transform_transaction_data
from src.load.transactions_to_db import insert_transactions
import src.config as config

def main():
    conn = connect_db()
    if conn:
        try:
            # Drop existing transaction tables, create new ones
            drop_tables(conn, 'senate_transactions', 'house_transactions')
            create_tables(conn, 'src/db/tables/transactions.sql')

            # Fetch transactions
            senate_transactions = fetch_transactions(config.SENATE_CSV_URL)
            house_transactions = fetch_transactions(config.HOUSE_CSV_URL)
            
            # Transform transactions
            senate_transactions = [transform_transaction_data(t) for t in senate_transactions]
            house_transactions = [transform_transaction_data(t) for t in house_transactions]
            
            # Insert transactions
            insert_transactions(senate_transactions, conn, 'senate_transactions')
            insert_transactions(house_transactions, conn, 'house_transactions')
            
            # Apply manual corrections
            apply_manual_corrections(conn, 'src/db/manual_corrections/transactions_corrections.sql')
            
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    main()
