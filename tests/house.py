import csv
import requests

house_csv_url = "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.csv"

class HouseTransaction:
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
        if row:
            transactions.append(HouseTransaction(row, field_map))
    
    return transactions

def print_transactions(transactions, count=5):
    print(f"Total number of transactions: {len(transactions)}\n")
    print("First few transactions:")
    for transaction in transactions[:count]:
        print("--------------------------------------------------")
        print("Amount:            ", transaction.amount)
        print("AssetDescription:  ", transaction.asset_description)
        print("CapGainsOver200Usd:", transaction.cap_gains_over_200_usd)
        print("DisclosureDate:    ", transaction.disclosure_date)
        print("DisclosureYear:    ", transaction.disclosure_year)
        print("District:          ", transaction.district)
        print("Industry:          ", transaction.industry)
        print("Owner:             ", transaction.owner)
        print("Party:             ", transaction.party)
        print("PtrLink:           ", transaction.ptr_link)
        print("Representative:    ", transaction.representative)
        print("Sector:            ", transaction.sector)
        print("State:             ", transaction.state)
        print("Ticker:            ", transaction.ticker)
        print("TransactionDate:   ", transaction.transaction_date)
        print("Type:              ", transaction.type)
    print("--------------------------------------------------")

if __name__ == "__main__":
    transactions = fetch_transactions(house_csv_url)
    print_transactions(transactions)
