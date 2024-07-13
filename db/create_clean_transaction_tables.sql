DROP TABLE IF EXISTS senate_transactions;
DROP TABLE IF EXISTS house_transactions;

CREATE TABLE senate_transactions (
    transaction_date DATE,
    owner VARCHAR(10),
    ticker VARCHAR(30),
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
    disclosure_date DATE
);

CREATE TABLE house_transactions (
    transaction_date DATE,
    owner VARCHAR(10),
    ticker VARCHAR(30),
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
    disclosure_year INTEGER
);

