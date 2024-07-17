import datetime

def convert_date(transaction_date_str, disclosure_date_str):
    # Convert disclosure date to get the correct year
    try:
        disclosure_date = datetime.datetime.strptime(disclosure_date_str, '%m/%d/%Y').date()
    except ValueError:
        print(f"Failed to parse disclosure date: {disclosure_date_str}")
        return None

    # Try to parse the transaction date in MM/DD/YYYY format
    try:
        return datetime.datetime.strptime(transaction_date_str, '%m/%d/%Y').date()
    except ValueError:
        # If MM/DD/YYYY fails, check if transaction date needs correction
        if transaction_date_str.startswith('0') or len(transaction_date_str) > 10:
            # Extract the year from the successfully parsed disclosure date
            year = disclosure_date.year
            # Assuming the day and month in transaction_date_str are correct, just the year is wrong
            month_day = transaction_date_str[-5:]
            corrected_date_str = f"{year}-{month_day}"
            try:
                corrected_date = datetime.datetime.strptime(corrected_date_str, '%Y-%m-%d').date()
                print(f"Corrected transaction date from {transaction_date_str} to {corrected_date_str}")
                return corrected_date
            except ValueError:
                print(f"Failed to convert using disclosure year: {corrected_date_str}")
                return None
        else:
            # If no corrections are applicable, attempt standard YYYY-MM-DD format
            try:
                return datetime.datetime.strptime(transaction_date_str, '%Y-%m-%d').date()
            except ValueError:
                print(f"Unhandled date format: {transaction_date_str}")
                return None
