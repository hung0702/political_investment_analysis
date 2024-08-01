UPDATE house_transactions
    SET party = 'Republican'
    WHERE representative ILIKE '%Eric%Burlison%'
        OR representative ILIKE '%Max%Miller%'
        OR representative ILIKE '%Thomas%H%Kean%';

UPDATE house_transactions
    SET party = 'Democrat'
    WHERE representative ILIKE '%Shri%Thanedar%'
        OR representative ILIKE '%Jared%Moskowitz%'
        OR representative ILIKE '%Jonathan%Jackson%'
        OR representative ILIKE '%Morgan%Mcgarvey%';

UPDATE house_transactions
    SET clean_ticker = NULL, crypto = True 
    WHERE ticker = 'GBTC';

UPDATE senate_transactions
    SET clean_ticker = NULL, crypto = True 
    WHERE ticker = 'GBTC';

UPDATE house_transactions
    SET clean_ticker = 'AAPL'
    WHERE ticker = 'AAPl';

UPDATE house_transactions
    SET clean_ticker = 'T'
    WHERE asset_description LIKE '%AT&T%' OR asset_description LIKE '%A T & T%';

UPDATE house_transactions
    SET clean_ticker = 'ZBH'
    WHERE asset_description LIKE '%Zimmer Biomet%' AND clean_ticker IS NULL;

UPDATE house_transactions
    SET clean_ticker = 'CWEN'
    WHERE asset_description LIKE '%Clearway%' AND clean_ticker IS NULL;
    
UPDATE house_transactions
    SET clean_ticker = 'BAC'
    WHERE asset_description LIKE '%Bank of America%' AND asset_description NOT ILIKE '%MTN%' AND clean_ticker IS NULL;

UPDATE house_transactions
    SET amount = '$1,001 - $15,000'
    WHERE amount IN ('$1,000 - $15,000', '$1,001 -');

UPDATE house_transactions
    SET amount = '$15,001 - $50,000'
    WHERE amount = '$15,000 - $50,000';

UPDATE house_transactions
    SET amount = '$1,000,001 - $5,000,000'
    WHERE amount = '$1,000,000 - $5,000,000';

UPDATE house_transactions
    SET amount = '$1,000,001 - $5,000,000'
    WHERE amount = '$1,000,000 +';

UPDATE house_transactions
    SET amount = '$15,001 - $50,000'
    WHERE amount = '$15,000 - $50,000';

UPDATE house_transactions
    SET amount = '$15,001 - $50,000'
    WHERE amount = '$15,000 - $50,000';

