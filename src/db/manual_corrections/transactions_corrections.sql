UPDATE house_transactions
    SET clean_ticker = NULL, crypto = True 
    WHERE ticker = 'GBTC';

UPDATE senate_transactions
    SET clean_ticker = NULL, crypto = True 
    WHERE ticker = 'GBTC';