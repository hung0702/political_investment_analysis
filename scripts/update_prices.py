import sys
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure the root directory is in sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from src.load.connection import connect_db
from src.extract.get_price_data import get_prices_batch
from src.load.price_data_to_db import update_price_data
from src.load.connection import connect_db, apply_manual_corrections

load_dotenv()

def fetch_price_requests(cursor, limit=10000):
    cursor.execute("""
        SELECT clean_ticker, transaction_date
        FROM price_data
        WHERE open IS NULL
            AND NOT (clean_ticker = 'VVC' and transaction_date = '2014-05-07')
        ORDER BY transaction_date ASC
        LIMIT %s
    """, (limit,))
    return cursor.fetchall()

def get_prices_threaded(cursor, ticker_date_pairs, max_workers=100):
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
            # Fetch price requests for rows where 'open' is NULL
            price_requests = fetch_price_requests(cursor)
            if price_requests:
                print(f"Fetching prices for {len(price_requests)} ticker-date pairs with NULL open prices...")
                get_prices_threaded(cursor, price_requests)
            else:
                print("No rows with NULL open prices found.")

            apply_manual_corrections(conn, 'src/db/manual_corrections/all_transactions_priced_corrections.sql')

        except Exception as e:
            print(f"An error occurred: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()