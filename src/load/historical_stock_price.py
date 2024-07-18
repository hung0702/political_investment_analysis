import psycopg2
import yfinance as yf
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


load_dotenv()

def round_to_5_decimals(value):
    return Decimal(str(value)).quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)

def fetch_price_requests(cursor, limit=1000):
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

def get_prices_batch(ticker, date):
    thread_id = threading.current_thread().name  # Get current thread's name
    print(f"{thread_id}: Processing {ticker} for {date}")
    
    stock = yf.Ticker(ticker)
    end_date = date + timedelta(days=1)  # Ensure the end date is included
    hist = stock.history(start=date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
    if not hist.empty:
        price_data = {
            'open': round_to_5_decimals(hist['Open'].iloc[0]),
            'high': round_to_5_decimals(hist['High'].iloc[0]),
            'low': round_to_5_decimals(hist['Low'].iloc[0]),
            'close': round_to_5_decimals(hist['Close'].iloc[0]),
            'adjusted_close': round_to_5_decimals(hist['Close'].iloc[0] + hist['Dividends'].iloc[0] / (1 + hist['Stock Splits'].iloc[0])),
            'volume': int(hist['Volume'].iloc[0])
        }
    else:
        print(f"No data found for ticker: {ticker} on {date}")
        price_data = None
    return (ticker, date, price_data)

def update_price_data(cursor, ticker, date, price_data):
    current_timestamp = datetime.now()
    try:
        if price_data is None:
            cursor.execute("""
                UPDATE price_data
                SET requested = TRUE, requested_date = %s
                WHERE clean_ticker = %s AND transaction_date = %s
            """, (current_timestamp, ticker, date))
        else:
            cursor.execute("""
                UPDATE price_data
                SET open = %s, close = %s, high = %s, low = %s, adjusted_close = %s, volume = %s,
                    requested = TRUE, requested_date = %s
                WHERE clean_ticker = %s AND transaction_date = %s
            """, (price_data['open'], price_data['close'], price_data['high'], price_data['low'],
                price_data['adjusted_close'], price_data['volume'], current_timestamp, 
                ticker, date))
    except Exception as e:
        print(f"An error occurred for ticker: {ticker} on {date}: {str(e)}")
        cursor.connection.rollback()


def get_prices_threaded(cursor, ticker_date_pairs, max_workers=100):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(get_prices_batch, ticker, date) for ticker, date in ticker_date_pairs]
        for future in as_completed(futures):
            ticker, date, price_data = future.result()
            update_price_data(cursor, ticker, date, price_data)
            cursor.connection.commit()

def main():
    try:
        conn = psycopg2.connect(os.getenv('DB_CONFIG'))
        conn.autocommit = False
        cursor = conn.cursor()
        price_requests = fetch_price_requests(cursor)
        if price_requests:
            print(f"Fetching prices for {len(price_requests)} ticker-date pairs...")
            get_prices_threaded(cursor, price_requests)
        else:
            print("No more unrequested price data.")
    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
