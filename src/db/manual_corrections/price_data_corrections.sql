UPDATE price_data 
    SET requested_date = CURRENT_TIMESTAMP, requested = True 
    WHERE clean_ticker = 'VVC'
        AND transaction_date = '2014-05-07';