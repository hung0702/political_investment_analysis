CREATE TABLE all_transactions_priced AS
SELECT
    t.transaction_id,
    t.transaction_date,
    t.owner,
    t.clean_ticker,
    t.asset_description,
    t.type,
    CASE
        WHEN asset_description ILIKE '% call%' OR asset_description ILIKE '% put%' AND NOT asset_description ILIKE '%putwrite%' THEN 'option'
        WHEN crypto = TRUE THEN 'crypto'
        WHEN asset_type IS NOT NULL THEN LOWER(asset_type)
        ELSE 'stock'
    END AS asset_type,
    t.amount,
    t.party,
    t.state,
    t.industry,
    t.sector,
    t.legislator,
    t.chamber,
    p.open,
    p.close,
    p.high,
    p.low,
    p.adjusted_close
FROM
    (SELECT transaction_id, transaction_date, owner, clean_ticker, asset_description, type, NULL AS asset_type, amount, party, state, industry, sector, representative AS legislator, crypto, 'house' AS chamber FROM house_transactions
    UNION ALL
    SELECT transaction_id, transaction_date, owner, clean_ticker, asset_description, type, asset_type, amount, party, state, industry, sector, senator AS legislator, crypto, 'senate' AS chamber FROM senate_transactions) t
LEFT JOIN price_data p USING (clean_ticker, transaction_date)
WHERE
    clean_ticker IS NOT NULL
ORDER BY transaction_date ASC;

CREATE UNIQUE INDEX idx_all_transactions_priced ON all_transactions_priced(transaction_id, chamber);