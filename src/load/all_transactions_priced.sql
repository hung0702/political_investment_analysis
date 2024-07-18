CREATE MATERIALIZED VIEW all_transactions_priced AS
SELECT
    COALESCE(s.transaction_id, h.transaction_id) AS transaction_id,
    COALESCE(s.transaction_date, h.transaction_date) AS transaction_date,
    COALESCE(s.owner, h.owner) AS owner,
    COALESCE(s.clean_ticker, h.clean_ticker) AS clean_ticker,
    COALESCE(s.asset_description, h.asset_description) AS asset_description,
    COALESCE(s.type, h.type) AS type,
    CASE
        WHEN s.asset_type IS NOT NULL THEN LOWER(s.asset_type)
        WHEN h.asset_description ILIKE '% call%' OR h.asset_description ILIKE '% put%' AND NOT h.asset_description ILIKE '%putwrite%' THEN 'option'
        WHEN h.crypto = TRUE THEN 'crypto'
        ELSE 'stock'
    END AS asset_type,
    COALESCE(s.amount, h.amount) AS amount,
    COALESCE(s.party, h.party) AS party,
    COALESCE(s.state, h.state) AS state,
    COALESCE(s.industry, h.industry) AS industry,
    COALESCE(s.sector, h.sector) AS sector,
    COALESCE(s.senator, h.representative) AS member,
    CASE
        WHEN s.transaction_id IS NOT NULL THEN 'senate'
        WHEN h.transaction_id IS NOT NULL THEN 'house'
    END AS chamber,
    p.open,
    p.close,
    p.high,
    p.low,
    p.adjusted_close
FROM
    senate_transactions s
FULL OUTER JOIN house_transactions h ON s.transaction_id = h.transaction_id
LEFT JOIN price_data p ON COALESCE(s.clean_ticker, h.clean_ticker) = p.clean_ticker AND COALESCE(s.transaction_date, h.transaction_date) = p.transaction_date
WHERE
    COALESCE(s.clean_ticker, h.clean_ticker) IS NOT NULL;

CREATE UNIQUE INDEX idx_all_transactions_priced ON all_transactions_priced(transaction_id, chamber);

DELETE FROM senate_transactions
WHERE asset_type IN ('Corporate Bond', 'Cryptocurrency', 'PDF Disclosed Filing', 'Stock Option');

UPDATE senate_transactions
SET asset_type = 'other'
WHERE asset_type = 'Other Securities';

