import re

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
