import yfinance as yf
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
import threading

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