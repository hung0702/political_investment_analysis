CREATE TABLE price_data (
    clean_ticker VARCHAR (10),
    transaction_date DATE, 
    open DECIMAL(8,4),
    close DECIMAL(8,4),
    high DECIMAL(8,4),
    low DECIMAL(8,4),
    adjusted_close DECIMAL(8,4),
    volume BIGINT,
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    requested_date TIMESTAMP,
    requested BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (clean_ticker, transaction_date)
);

INSERT INTO price_data (clean_ticker, transaction_date, requested)
SELECT DISTINCT clean_ticker, transaction_date, FALSE
FROM (
    SELECT DISTINCT clean_ticker, transaction_date
    FROM senate_transactions
    UNION
    SELECT DISTINCT clean_ticker, transaction_date
    FROM house_transactions
) AS combined_transactions
WHERE clean_ticker IS NOT NULL
ORDER BY transaction_date ASC;
