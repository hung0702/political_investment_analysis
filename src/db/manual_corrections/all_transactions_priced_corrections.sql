DELETE FROM senate_transactions
WHERE asset_type IN ('Corporate Bond', 'Cryptocurrency', 'PDF Disclosed Filing', 'Stock Option');

UPDATE senate_transactions
SET asset_type = 'other'
WHERE asset_type = 'Other Securities';