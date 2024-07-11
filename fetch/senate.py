import csv
import requests

class Transaction:
    def __init__(self, data, field_map):
        for key in field_map:
            # Set attributes of the class to CSV headers
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
        if row:  # Check if the row is not empty
            transactions.append(Transaction(row, field_map))
    
    return transactions

def print_transactions(transactions, count=5):
    print(f"Total number of transactions: {len(transactions)}\n")
    print("First few transactions:")
    for transaction in transactions[:count]:
        print("--------------------------------------------------")
        print("Amount:           ", transaction.amount)
        print("AssetDescription: ", transaction.asset_description)
        print("AssetType:        ", transaction.asset_type)
        print("Comment:          ", transaction.comment)
        print("DisclosureDate:   ", transaction.disclosure_date)
        print("Industry:         ", transaction.industry)
        print("Owner:            ", transaction.owner)
        print("Party:            ", transaction.party)
        print("PtrLink:          ", transaction.ptr_link)
        print("Sector:           ", transaction.sector)
        print("Senator:          ", transaction.senator)
        print("State:            ", transaction.state)
        print("Ticker:           ", transaction.ticker)
        print("TransactionDate:  ", transaction.transaction_date)
        print("Type:             ", transaction.type)
    print("--------------------------------------------------")

if __name__ == "__main__":
    senate_csv_url = "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.csv"
    transactions = fetch_transactions(senate_csv_url)
    print_transactions(transactions)
