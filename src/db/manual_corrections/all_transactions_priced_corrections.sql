DELETE FROM all_transactions_priced
WHERE asset_type IN ('corporate bond', 'cryptocurrency', 'pdf disclosed filing', 'option');

UPDATE all_transactions_priced
SET asset_type = 'other'
WHERE asset_type = 'other securities';