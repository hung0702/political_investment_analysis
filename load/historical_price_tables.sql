CREATE TABLE price_data (
    price_id SERIAL PRIMARY KEY,
    chamber VARCHAR(6) CHECK (chamber IN ('house', 'senate')),
    transaction_id INTEGER,
    open NUMERIC,
    close NUMERIC,
    high NUMERIC,
    low NUMERIC,
    adjusted_close NUMERIC,
    volume BIGINT,
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (chamber, transaction_id),
    CONSTRAINT fk_house_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES house_transactions(transaction_id)
        WHEN chamber = 'house',
    CONSTRAINT fk_senate_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES senate_transactions(transaction_id)
        WHEN chamber = 'senate'