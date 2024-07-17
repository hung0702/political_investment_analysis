# test_transaction_price.py

import sys
import os
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.transactions import fetch_transactions, is_cryptocurrency, clean_ticker
import config
import yfinance as yf
from datetime import timedelta

def round_to_4_decimals(value):
    return Decimal(str(value)).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)

def get_stock_prices(ticker_date_pairs):
    price_data_dict = {}
    for cleaned_ticker, date in ticker_date_pairs:
        try:
            data = yf.Ticker(cleaned_ticker)
            start_date = date
            end_date = date + timedelta(days=1)
            historical_data = data.history(start=start_date, end=end_date)

            if historical_data.empty:
                print(f"No data found for ticker: {cleaned_ticker} on {date}")
                continue

            price_data = historical_data.iloc[0]
            
            adjusted_close = (price_data['Close'] + price_data['Dividends']) / (1 + price_data['Stock Splits'])

            price_data_dict[(cleaned_ticker, date)] = {
                'open': round_to_4_decimals(price_data['Open']),
                'high': round_to_4_decimals(price_data['High']),
                'low': round_to_4_decimals(price_data['Low']),
                'close': round_to_4_decimals(price_data['Close']),
                'adjusted_close': round_to_4_decimals(adjusted_close),
                'volume': int(price_data['Volume'])
            }
        except Exception as e:
            print(f"Error fetching data for {cleaned_ticker} on {date}: {str(e)}")

    return price_data_dict

def get_transactions_for_testing(transactions, start=0, count=15):
    sorted_transactions = sorted(transactions, key=lambda x: len(x.ticker) if x.ticker else 0, reverse=True)
    return sorted_transactions[start:start+count]

def main():
    print("Fetching Senate transactions...")
    senate_transactions = fetch_transactions(config.SENATE_CSV_URL)
    print("Fetching House transactions...")
    house_transactions = fetch_transactions(config.HOUSE_CSV_URL)

    all_transactions = senate_transactions + house_transactions

    test_transactions = get_transactions_for_testing(all_transactions)

    print("\nTransactions for testing:")
    ticker_date_pairs = set()
    for transaction in test_transactions:
        if is_cryptocurrency(transaction.ticker):
            print(f"Skipping cryptocurrency: {transaction.ticker}")
            continue
        cleaned_ticker = clean_ticker(transaction.ticker, transaction.asset_description)
        if cleaned_ticker:
            ticker_date_pairs.add((cleaned_ticker, transaction.transaction_date))
        print(f"Ticker: {transaction.ticker}, Cleaned: {cleaned_ticker}, Date: {transaction.transaction_date}")
        print(f"Asset Description: {transaction.asset_description}")

    print(f"\nUnique ticker-date pairs: {len(ticker_date_pairs)}")
    print("\nFetching stock price data:")
    price_data_dict = get_stock_prices(ticker_date_pairs)

    print("\nStock price data results:")
    for transaction in test_transactions:
        if is_cryptocurrency(transaction.ticker):
            print(f"\nSkipping cryptocurrency: {transaction.ticker}")
        else:
            cleaned_ticker = clean_ticker(transaction.ticker, transaction.asset_description)
            if cleaned_ticker:
                price_data = price_data_dict.get((cleaned_ticker, transaction.transaction_date))
                if price_data:
                    print(f"\nOriginal ticker: {transaction.ticker}")
                    print(f"Cleaned ticker: {cleaned_ticker}")
                    print(f"Date: {transaction.transaction_date}")
                    print("Price data:")
                    for key, value in price_data.items():
                        print(f"  {key}: {value}")
                else:
                    print(f"\nNo price data available for {cleaned_ticker} on {transaction.transaction_date}")
            else:
                print(f"\nInvalid ticker: {transaction.ticker}")

if __name__ == "__main__":
    main()