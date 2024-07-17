'''This script takes 1000 rows of ticker and transaction_dates from unrequested
public.price_data records to request from yfinance. The tickers are then batched
by day, and cycles through valid proxies to fetch data.
'''
import psycopg2
import yfinance as yf
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from dotenv import load_dotenv
import os
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

load_dotenv()

proxies = [
    "211.253.36.172:5005",
    "122.155.165.191:3128",
    "134.209.144.177:80",
    "116.202.165.119:3128",
    "72.10.160.170:10919",
    "203.142.77.226:8080",
    "8.219.97.248:80",
    "103.80.237.211:3888",
    "125.99.106.250:3128",
    "104.131.91.60:3128",
    "72.10.164.178:3001",
    "185.191.236.162:3128",
    "51.158.169.52:29976",
    "197.248.86.237:32650",
    "103.149.194.222:80",
    "67.43.236.20:5103",
    "103.177.235.132:83",
    "103.108.156.38:8080",
    "72.10.160.90:19101",
    "62.23.184.84:8080",
    "61.29.96.146:80",
    "45.95.232.128:3128",
    "103.165.234.46:8080",
    "134.122.26.11:80",
    "177.73.68.150:8080",
    "67.43.227.227:26373",
    "103.148.130.3:8085",
    "103.156.17.83:8080",
    "103.110.10.190:3128",
    "103.167.170.202:1111"
]


def round_to_4_decimals(value):
    return Decimal(str(value)).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)


def fetch_price_requests(cursor, limit=1000):
    cursor.execute("""
        SELECT clean_ticker, transaction_date
        FROM price_data
        WHERE NOT requested
        ORDER BY transaction_date
        LIMIT %s
    """, (limit,))
    return cursor.fetchall()


def validate_proxy(proxy):
    try:
        response = requests.get('https://query2.finance.yahoo.com/v8/finance/chart/SPY', proxies={"http": proxy, "https": proxy}, timeout=5)
        return response.status_code == 200
    except:
        return False


def get_stock_prices_batch(date, batch_tickers, proxy):
    price_data_dict = {}
    try:
        ticker_str = " ".join(batch_tickers)
        start_date = date
        end_date = date + timedelta(days=1)

        data = yf.download(ticker_str, start=start_date, end=end_date, proxy=proxy, group_by='ticker')

        if data.empty:
            print(f"No data found for tickers on {date}")
            for ticker in batch_tickers:
                price_data_dict[(ticker, date)] = None
            return price_data_dict

        for ticker in batch_tickers:
            if ticker in data.columns:
                ticker_data = data[ticker]
                if not ticker_data.empty:
                    price_data = ticker_data.iloc[0]
                    adjusted_close = (price_data['Close'] + price_data['Dividends']) / (1 + price_data['Stock Splits'])
                    price_data_dict[(ticker, date)] = {
                        'open': round_to_4_decimals(price_data['Open']),
                        'high': round_to_4_decimals(price_data['High']),
                        'low': round_to_4_decimals(price_data['Low']),
                        'close': round_to_4_decimals(price_data['Close']),
                        'adjusted_close': round_to_4_decimals(adjusted_close),
                        'volume': int(price_data['Volume'])
                    }
                else:
                    print(f"No data found for ticker: {ticker} on {date}")
                    price_data_dict[(ticker, date)] = None
            else:
                print(f"No data found for ticker: {ticker} on {date}")
                price_data_dict[(ticker, date)] = None
    except Exception as e:
        print(f"Error fetching data for batch on {date} using proxy {proxy}: {str(e)}")
        for ticker in batch_tickers:
            price_data_dict[(ticker, date)] = None

    return price_data_dict


def get_stock_prices_parallel(ticker_date_pairs, batch_size=1000, max_workers=5):
    price_data_dict = {}
    
    # Group ticker_date_pairs by date
    date_groups = {}
    for clean_ticker, date in ticker_date_pairs:
        if date not in date_groups:
            date_groups[date] = []
        date_groups[date].append(clean_ticker)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {}
        for date, tickers in date_groups.items():
            for i in range(0, len(tickers), batch_size):
                batch_tickers = tickers[i:i+batch_size]
                proxy = random.choice(proxies)
                future = executor.submit(get_stock_prices_batch, date, batch_tickers, proxy)
                future_to_batch[future] = (date, batch_tickers)

        for future in as_completed(future_to_batch):
            date, batch_tickers = future_to_batch[future]
            try:
                batch_result = future.result()
                price_data_dict.update(batch_result)
            except Exception as e:
                print(f"Batch processing failed for date {date}: {str(e)}")
                for ticker in batch_tickers:
                    price_data_dict[(ticker, date)] = None
            
            # Add a small delay to avoid overwhelming the API
            time.sleep(1)

    return price_data_dict


def update_price_data(cursor, price_data_dict):
    current_timestamp = datetime.now()
    
    for (clean_ticker, transaction_date), price_data in price_data_dict.items():
        if price_data is None:
            # Update rows with no available data
            cursor.execute("""
                UPDATE price_data
                SET requested = TRUE, requested_date = %s
                WHERE clean_ticker = %s AND transaction_date = %s
            """, (current_timestamp, clean_ticker, transaction_date))

        else:
            # Update rows with available price data
            cursor.execute("""
                UPDATE price_data
                SET open = %s, close = %s, high = %s, low = %s, adjusted_close = %s, volume = %s,
                    requested = TRUE, upload_date = %s
                WHERE clean_ticker = %s AND transaction_date = %s
            """, (price_data['open'], price_data['close'], price_data['high'], price_data['low'],
                  price_data['adjusted_close'], price_data['volume'], current_timestamp,
                  clean_ticker, transaction_date))

def main():
    try:
        conn = psycopg2.connect(os.getenv('DB_CONFIG'))
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        return
    
    cursor = conn.cursor()

    try:
        while True:
            price_requests = fetch_price_requests(cursor)
            if not price_requests:
                print("No more unrequested price data.")
                break

            print(f"Fetching prices for {len(price_requests)} ticker-date pairs...")
            price_data_dict = get_stock_prices_parallel(price_requests, batch_size=1000, max_workers=5)

            print("Updating price data in the database...")
            update_price_data(cursor, price_data_dict)

            conn.commit()
            print(f"Processed {len(price_requests)} ticker-date pairs.")
            
            print("Fetching next batch of price requests...")

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()