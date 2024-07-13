import csv
import datetime
import requests
from config import HOUSE_CSV_URL, SENATE_CSV_URL

def convert_date(date_str):
    try:
        # Try standard YYYY-MM-DD format
        return datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        try:
            # Fallback to MM/DD/YYYY format
            return datetime.datetime.strptime(date_str, '%m/%d/%Y').date()
        except ValueError:
            # Handle year typos where 4 digit is erroneous, e.g., '20221-11-18'
            if date_str[4] != '-':
                year = date_str[:3] + date_str[4]
                corrected_date_string = f"{year}-{date_str[6:8]}-{date_str[9:]}"
                print(f"Date typo changed from {date_str} to {corrected_date_string}")
                return datetime.datetime.strptime(corrected_date_string, '%Y-%m-%d').date()
            else:
                print(f"Unhandled date format: {date_str}")
                return None

class Transaction:
    def __init__(self, data, field_map):
        date_fields = ['transaction_date', 'disclosure_date']
        boolean_fields = ['cap_gains_over_200_usd']

        for key in field_map:
            if key in date_fields:
                setattr(self, key, convert_date(data[field_map[key]]))
            elif key in boolean_fields:
                value = data[field_map[key]].strip().title()
                setattr(self, key, True if value == 'True' else False)
            else:
                setattr(self, key, data[field_map[key]])

def fetch_transactions(url):
    response = requests.get(url)
    response.raise_for_status()  # Raises an HTTPError for bad responses

    # Decode the CSV content
    content = response.content.decode('utf-8')
    csv_reader = csv.reader(content.splitlines())
    
    header = next(csv_reader)
    field_map = {field.lower(): index for index, field in enumerate(header)}

    transactions = []
    for row in csv_reader:
        if row:
            transactions.append(Transaction(row, field_map))
    
    return transactions

def print_transactions(transactions, count=5):
    print(f"Total number of transactions: {len(transactions)}\n")
    print("First few transactions:")
    for transaction in transactions[:count]:
        print("--------------------------------------------------")
        for key, value in vars(transaction).items():
            print(f"{key.title()}: {value}")
        print("--------------------------------------------------")

if __name__ == "__main__":
    house_transactions = fetch_transactions(HOUSE_CSV_URL)
    senate_transactions = fetch_transactions(SENATE_CSV_URL)
    # print_transactions(house_transactions)
    # print_transactions(senate_transactions)
