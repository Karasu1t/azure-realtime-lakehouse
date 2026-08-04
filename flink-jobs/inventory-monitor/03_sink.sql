-- format-version 2 + upsert lets repeated updates for the same product_id
-- overwrite the row instead of appending a new one, so this table always
-- reflects "current stock", not a full event history.
CREATE TABLE stock_status (
  product_id    STRING,
  current_stock BIGINT,
  low_stock     BOOLEAN,
  updated_at    TIMESTAMP(3),
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
  'format-version'        = '2',
  'write.upsert.enabled'  = 'true'
);
