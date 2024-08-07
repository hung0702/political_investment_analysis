"""These functions transform transaction data, which are sometimes malformed,
have typoes, or are otherwise not readily convertible
"""
from datetime import datetime
import re

"""Fix date strings from the CSV data, which could be in different formats or malformed
"""
def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').date()
    except ValueError:
        print(f"Could not parse date: {date_str}")
        return None

def convert_date(disclosure_date_str, transaction_date_str=None):
    disclosure_date = parse_date(disclosure_date_str)
    if not disclosure_date:
        print(f"Could not parse date: {disclosure_date_str}")
        return None

    if transaction_date_str is None and disclosure_date:
        return disclosure_date

    # Senate transaction date strings are always 'MM/DD/YYYY'
    if '/' in transaction_date_str:
        try:
            transaction_date = datetime.strptime(transaction_date_str, '%m/%d/%Y').date()

            if disclosure_date >= transaction_date:
                return transaction_date

            else:
                print(f"Warning: disclosure date {disclosure_date} is before transaction date {transaction_date}")
                return transaction_date
            
        except ValueError:
            print(f"Could not parse transaction date: {transaction_date_str}")
            return None

    # House transaction date strings are always 'YYYY-MM-DD'
    else:
        if len(transaction_date_str) == 10 and not transaction_date_str.startswith('0'):
            return datetime.strptime(transaction_date_str, '%Y-%m-%d').date()

        elif len(transaction_date_str) != 10 or transaction_date_str.startswith('0'):
            corrected_date_str = f"{disclosure_date.year}-{transaction_date_str[-5:]}"
            transaction_date = datetime.strptime(corrected_date_str, '%Y-%m-%d').date()

            if disclosure_date >= transaction_date:
                return transaction_date

            else:
                print(f"Warning: disclosure date {disclosure_date} is before transaction date {transaction_date}")
                return transaction_date
        
        else:
            print(f"Unhandled transaction date format: {transaction_date_str}")
            return None

"""Clean ticker symbols; which may not always be given in the correct format, or
may be given in a different field
"""

def extract_ticker(text):
    match = re.search(r'\b([A-Z]{1,5})([-.]?[A-Z])?\b', text)
    return match.group(0) if match else None

def is_cryptocurrency(ticker):
    return '-USD' in ticker.upper()

def clean_ticker(ticker, asset_description):
    if is_cryptocurrency(ticker):
        return None

    if re.match(r'^[A-Z]{1,5}(-[A-Z])?$', ticker):
        return ticker

    if len(ticker) > 5 and asset_description:
        # Sometimes congressmembers enter the ticker in asset_description
        potential_ticker = extract_ticker(asset_description)
        
        if potential_ticker:
            ticker = potential_ticker

    cleaned = re.sub(r'[^A-Za-z0-9.-]', '', ticker)
    
    base_match = re.match(r'^([A-Za-z]{1,5})', cleaned)
    if not base_match:
        return None

    base_ticker = base_match.group(0).upper()
    
    class_match = re.search(r'[.-]([A-Z])$', cleaned)
    if class_match:
        return f"{base_ticker}-{class_match.group(1)}"

"""This last function finalizes transactions transformation
"""

def transform_transaction_data(transaction):
    transaction.transaction_date = convert_date(transaction.disclosure_date, transaction.transaction_date)
    transaction.disclosure_date = convert_date(transaction.disclosure_date)
    transaction.clean_ticker = clean_ticker(transaction.ticker, transaction.asset_description)
    transaction.crypto = is_cryptocurrency(transaction.ticker)
    return transaction
