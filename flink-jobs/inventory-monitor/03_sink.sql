-- format-version 2 + upsert lets repeated updates for the same product_id
-- overwrite the row instead of appending a new one, so this table always
-- reflects "current stock", not a full event history.
CREATE TABLE IF NOT EXISTS stock_status (
  product_id    STRING,
  current_stock BIGINT,
  low_stock     BOOLEAN,
  -- TIMESTAMP_LTZ to match 02_source.sql's event_time (MAX(event_time) in
  -- 04_pipeline.sql produces this type); maps to Iceberg's timestamptz.
  updated_at    TIMESTAMP_LTZ(3),
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
  'format-version'        = '2',
  'write.upsert.enabled'  = 'true'
);
