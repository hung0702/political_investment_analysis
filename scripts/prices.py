"""
This script fetches and update stock price data 

1. Connects to specified database
2. Drops existing price-related tables and recreates them from specified SQL
   scripts; note that price
3. Fetches stock prices for transactions that haven't been requested yet, with a
   default limit of 1000 to avoid API rate-limiting (see footnote)
4. Uses a ThreadPoolExecutor with 100 workers by default
5. Updates the database with the fetched prices and marks them as requested
6. Applies manual SQL corrections from specified SQL scripts

Note:

Users should adjust the fetch limit and the number of workers based on their specific needs and constraints, such as API rate limits and system resources. This can be done by setting parameters in `src/extract/get_price_data.py` (e.g., increasing the limit to 21000 and workers to 500 for more extensive data fetching).
"""
import sys
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure the root directory is in sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from src.db.connection import connect_db, drop_tables, create_tables, apply_manual_corrections
from src.extract.get_price_data import get_prices_batch
from src.load.price_data_to_db import update_price_data

load_dotenv()

def fetch_price_requests(cursor, limit=25000): # adjust limit as necessary
    cursor.execute("""
        SELECT clean_ticker, transaction_date
        FROM price_data
        WHERE NOT requested 
            AND NOT (clean_ticker = 'VVC' and transaction_date =
        '2014-05-07') -- this specific price is >2.9e+20
        ORDER BY transaction_date ASC
        LIMIT %s
    """, (limit,))
    return cursor.fetchall()

def get_prices_threaded(cursor, ticker_date_pairs, max_workers=500): # adjust workers as necessary
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(get_prices_batch, ticker, date) for ticker, date in ticker_date_pairs]
        for future in as_completed(futures):
            ticker, date, price_data = future.result()
            update_price_data(cursor, ticker, date, price_data)
            cursor.connection.commit()

def main():
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            # Drop existing tables, create new ones
            drop_tables(conn, 'price_data', 'all_transactions_priced')
            create_tables(conn, 'src/db/tables/price_data.sql')
            
            # Fetch price requests
            price_requests = fetch_price_requests(cursor)
            if price_requests:
                print(f"Fetching prices for {len(price_requests)} ticker-date pairs...")
                get_prices_threaded(cursor, price_requests)
            else:
                print("No more unrequested price data.")
            
            # Apply manual corrections
            apply_manual_corrections(conn, 'src/db/manual_corrections/price_data_corrections.sql')

            # After price_data corrections, create all_transactions_priced table
            # and apply corrections to it
            create_tables(conn, 'src/db/tables/all_transactions_priced.sql')
            apply_manual_corrections(conn, 'src/db/manual_corrections/all_transactions_priced_corrections.sql')

        except Exception as e:
            print(f"An error occurred: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()
