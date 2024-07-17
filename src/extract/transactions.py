'''This script pulls CSV data from House Stock Watcher and Senate Stock Watcher
to build the inital list for transformation.
'''

import csv
import requests
from ..config import HOUSE_CSV_URL, SENATE_CSV_URL

class Transaction:
    def __init__(self, data, field_map):
        for key in field_map:
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


# def print_transactions(transactions, count=5):
#     print(f"Total number of transactions: {len(transactions)}\n")
#     print("First few transactions:")
#     for transaction in transactions[:count]:
#         print("--------------------------------------------------")
#         for key, value in vars(transaction).items():
#             print(f"{key.title()}: {value}")
#         print("--------------------------------------------------")

if __name__ == "__main__":
    house_transactions = fetch_transactions(HOUSE_CSV_URL)
    senate_transactions = fetch_transactions(SENATE_CSV_URL)
    # print_transactions(house_transactions)
    # print_transactions(senate_transactions)
