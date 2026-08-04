-- Event Hubs' Kafka-compatible endpoint, consumed with the plain Kafka
-- connector. Username is always the literal string "$ConnectionString";
-- the real secret is the connection string used as the password.
CREATE TABLE inventory_events (
  product_id  STRING,
  event_type  STRING,  -- 'SALE' or 'RESTOCK'
  quantity    INT,
  event_time  TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                       = 'kafka',
  'topic'                           = 'inventory-events',
  'properties.bootstrap.servers'    = '${EVENTHUBS_BOOTSTRAP_SERVERS}',
  'properties.security.protocol'    = 'SASL_SSL',
  'properties.sasl.mechanism'       = 'PLAIN',
  'properties.sasl.jaas.config'     = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="${EVENTHUBS_CONNECTION_STRING}";',
  'properties.group.id'             = 'inventory-monitor',
  'scan.startup.mode'               = 'latest-offset',
  'format'                          = 'json',
  'json.timestamp-format.standard'  = 'ISO-8601'
);
