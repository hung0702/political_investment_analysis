DELETE FROM all_transactions_priced
WHERE asset_type IN ('Corporate bond', 'Cryptocurrency', 'Pdf Disclosed Filing', 'Option');

UPDATE all_transactions_priced
SET asset_type = 'Other'
WHERE asset_type = 'Other Securities';

UPDATE all_transactions_priced
SET fewest_stocks = FLOOR(amount_lower/high)
WHERE open IS NOT NULL;

UPDATE all_transactions_priced
SET middle_stocks = FLOOR(((amount_higher + amount_lower)/2.0) / ((high+low)/2.0))
WHERE open IS NOT NULL;

UPDATE all_transactions_priced
SET most_stocks = FLOOR(amount_higher/low)
WHERE open IS NOT NULL;