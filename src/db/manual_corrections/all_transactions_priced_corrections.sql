DELETE FROM all_transactions_priced
WHERE asset_type IN ('Corporate bond', 'Cryptocurrency', 'Pdf Disclosed Filing', 'Option');

UPDATE all_transactions_priced
SET asset_type = 'Other'
WHERE asset_type = 'Other Securities';