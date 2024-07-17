from datetime import datetime

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