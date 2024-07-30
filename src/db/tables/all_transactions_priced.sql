CREATE TABLE all_transactions_priced AS
SELECT
    t.transaction_id,
    t.transaction_date,
    INITCAP(TRIM(t.owner)) AS owner,
    UPPER(TRIM(t.ticker)) AS ticker,
    UPPER(TRIM(t.clean_ticker)) AS clean_ticker,
    TRIM(t.asset_description) AS asset_description,
    INITCAP(TRIM(t.type)) AS type,
    CASE
        WHEN asset_description ILIKE '% call%' OR asset_description ILIKE '% put%' AND NOT asset_description ILIKE '%putwrite%' THEN 'Option'
        WHEN asset_type IS NOT NULL THEN INITCAP(asset_type)
        ELSE 'Stock'
    END AS asset_type,
    CASE
        WHEN amount LIKE '%$1,001%' THEN 1001
        WHEN amount LIKE '%$15,001%' THEN 15001
        WHEN amount LIKE '%$50,001%' THEN 50001
        WHEN amount LIKE '%$100,001%' THEN 100001
        WHEN amount LIKE '%$250,001%' THEN 250001
        WHEN amount LIKE '%$500,001%' THEN 500001
        WHEN amount LIKE '%$1,000,001%' THEN 1000001
        WHEN amount LIKE '%$5,000,001%' THEN 5000001
        WHEN amount LIKE '%$25,000,001%' THEN 25000001
        WHEN amount LIKE '%$50,000,000' THEN 50000000
    END AS amount_lower,
    CASE 
        WHEN amount LIKE '%$15,000' THEN 15000
        WHEN amount LIKE '%$50,000' THEN 50000
        WHEN amount LIKE '%$100,000' THEN 100000
        WHEN amount LIKE '%$250,000' THEN 250000
        WHEN amount LIKE '%$500,000' THEN 500000
        WHEN amount LIKE '%$1,000,000' THEN 1000000
        WHEN amount LIKE '%$5,000,000' THEN 5000000
        WHEN amount LIKE '%$25,000,000' THEN 25000000
        WHEN amount LIKE '%$50,000,000' THEN NULL
    END AS amount_higher,
    0 as fewest_stocks,
    0 as middle_stocks,
    0 as most_stocks,
    INITCAP(TRIM(t.party)) AS party,
    UPPER(TRIM(t.state)) AS state,
    UPPER(TRIM(t.district)) AS district,
    INITCAP(TRIM(t.industry)) AS industry,
    INITCAP(TRIM(t.sector)) AS sector,
    INITCAP(TRIM(t.legislator)) AS legislator,
    INITCAP(TRIM(t.chamber)) AS chamber,
    p.open,
    p.close,
    p.high,
    p.low,
    p.adjusted_close
FROM
    (SELECT transaction_id, transaction_date, owner, ticker, clean_ticker, asset_description, type, NULL AS asset_type, amount, party, state, district, industry, sector, representative AS legislator, 'house' AS chamber FROM house_transactions
    UNION ALL
    SELECT transaction_id, transaction_date, owner, ticker, clean_ticker, asset_description, type, asset_type, amount, party, state, NULL AS district, industry, sector, senator AS legislator, 'senate' AS chamber FROM senate_transactions) t
LEFT JOIN price_data p USING (clean_ticker, transaction_date)
WHERE
    clean_ticker IS NOT NULL
ORDER BY transaction_date ASC;

CREATE UNIQUE INDEX idx_all_transactions_priced ON all_transactions_priced(transaction_id, chamber);