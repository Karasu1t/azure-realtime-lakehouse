-- current_stock is a running sum of deltas seen since this job started,
-- not an absolute count synced from an external inventory system. The
-- simulator seeds each product with an initial RESTOCK event so the sum
-- starts from a known baseline.
INSERT INTO stock_status
SELECT
  product_id,
  SUM(CASE WHEN event_type = 'SALE' THEN -quantity ELSE quantity END) AS current_stock,
  SUM(CASE WHEN event_type = 'SALE' THEN -quantity ELSE quantity END) < ${LOW_STOCK_THRESHOLD} AS low_stock,
  MAX(event_time) AS updated_at
FROM inventory_events
GROUP BY product_id;
