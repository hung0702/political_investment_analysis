import psycopg2
from datetime import datetime

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

