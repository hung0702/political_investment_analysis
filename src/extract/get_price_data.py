import yfinance as yf
from datetime import datetime, timedelta, time
import threading
import pytz
from decimal import Decimal, ROUND_HALF_UP

def round_to_5_decimals(value):
    return Decimal(str(value)).quantize(Decimal('0.00001'), rounding=ROUND_HALF_UP)

def get_prices_batch(ticker, date):
    thread_id = threading.current_thread().name
    print(f"{thread_id}: Processing {ticker} for {date}")

    if not isinstance(date, datetime):
        date = datetime.combine(date, datetime.min.time())
    est = pytz.timezone('America/New_York')
    date_est = est.localize(date)
    date_utc = date_est.astimezone(pytz.UTC)
    end_date_utc = date_utc + timedelta(days=1)

    stock = yf.Ticker(ticker)
    hist = stock.history(start=date_utc.strftime('%Y-%m-%d'), end=end_date_utc.strftime('%Y-%m-%d'), interval='1d')
    
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

    return (ticker, date_est.date(), price_data)
