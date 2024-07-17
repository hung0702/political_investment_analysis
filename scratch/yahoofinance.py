import yfinance as yf
import datetime

# Define the ticker, start date, and end date
ticker = "LM-SG"
startDate = datetime.datetime(2023, 7, 14)
endDate = datetime.datetime(2023, 7, 15)  # Include July 14 in the output by setting endDate to July 15

# Fetch data for the ticker
data = yf.Ticker(ticker)

try:
    # Get historical data using the specified date range
    historical_data = data.history(start=startDate, end=endDate)
    print(historical_data)
except Exception as e:
    print(f"An error occurred: {e}")

# If no data is returned, check if the DataFrame is empty
if historical_data.empty:
    print("No data found for the given dates. Please check the dates or ticker symbol.")
