DROP TABLE IF EXISTS senate_transactions;
DROP TABLE IF EXISTS house_transactions;

CREATE TABLE senate_transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_date DATE,
    owner VARCHAR(10),
    ticker VARCHAR(30),
    clean_ticker VARCHAR (10),
    crypto BOOLEAN,
    asset_description VARCHAR(255),
    asset_type VARCHAR(30),
    type VARCHAR(20),
    amount VARCHAR(30),
    comment TEXT,
    party VARCHAR(20),
    state VARCHAR(2),
    industry VARCHAR(70),
    sector VARCHAR(30),
    senator VARCHAR(50),
    ptr_link VARCHAR(100),
    disclosure_date DATE,
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
-- note that handwritten disclosure reports contain many duplicates, so it's
-- duplicate records should be considered accurate
    UNIQUE (transaction_id, transaction_date, owner, ticker, asset_description, amount, ptr_link, senator, disclosure_date)
);

CREATE TABLE house_transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_date DATE,
    owner VARCHAR(10),
    ticker VARCHAR(30),
    clean_ticker VARCHAR (10),
    crypto BOOLEAN,
    asset_description VARCHAR(255),
    type VARCHAR(20),
    amount VARCHAR(30),
    party VARCHAR(20),
    state VARCHAR(2),
    industry VARCHAR(70),
    sector VARCHAR(30),
    cap_gains_over_200_usd BOOLEAN,
    representative VARCHAR(50),
    district VARCHAR(4),
    ptr_link VARCHAR(100),
    disclosure_date DATE,
    disclosure_year INTEGER,
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
-- note that handwritten disclosure reports contain many duplicates, so it's
-- duplicate records should be considered accurate
    UNIQUE (transaction_id, transaction_date, owner, ticker, asset_description, amount, ptr_link, representative, disclosure_date)
);