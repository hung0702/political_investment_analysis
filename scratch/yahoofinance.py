import yfinance as yf
from datetime import datetime, timedelta
import pytz

# Function to convert EST to UTC
def est_to_utc(est_date):
    est = pytz.timezone('America/New_York')
    utc = pytz.UTC
    est_date = est.localize(est_date)
    return est_date.astimezone(utc)

# Set up the test
ticker = 'BREIT'
est_date = datetime(2023, 3, 22)  # This is in EST

# Convert EST to UTC
utc_date = est_to_utc(est_date)
utc_end_date = est_to_utc(est_date + timedelta(days=1))

# Fetch data using UTC dates
stock = yf.Ticker(ticker)
hist = stock.history(start=utc_date.strftime('%Y-%m-%d'), end=utc_end_date.strftime('%Y-%m-%d'), interval='1d')

# Print the result
print(f"Data for {ticker} on {est_date.strftime('%Y-%m-%d')} (EST):")
print(hist)

# Print individual values
if not hist.empty:
    print("\nIndividual values:")
    print(f"Open: {hist['Open'].iloc[0]:.2f}")
    print(f"High: {hist['High'].iloc[0]:.2f}")
    print(f"Low: {hist['Low'].iloc[0]:.2f}")
    print(f"Close: {hist['Close'].iloc[0]:.2f}")
    print(f"Volume: {hist['Volume'].iloc[0]}")
else:
    print("No data available for this date.")