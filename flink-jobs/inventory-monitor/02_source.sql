-- Event Hubs' Kafka-compatible endpoint, consumed with the plain Kafka
-- connector. Username is always the literal string "$ConnectionString";
-- the real secret is the connection string used as the password.
CREATE TABLE inventory_events (
  product_id  STRING,
  event_type  STRING,  -- 'SALE' or 'RESTOCK'
  quantity    INT,
  -- TIMESTAMP_LTZ, not TIMESTAMP: the value carries a timezone (simulator
  -- sends UTC as a 'Z'-suffixed ISO-8601 string). Confirmed locally that
  -- Flink's 'json.timestamp-format.standard'='ISO-8601' only accepts a 'Z'
  -- suffix for this -- a numeric offset like '+00:00' silently parses to
  -- NULL (no error; json.ignore-parse-errors is what surfaces it as NULL
  -- instead of failing the job, which is how this was diagnosed).
  event_time  TIMESTAMP_LTZ(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                       = 'kafka',
  'topic'                           = 'inventory-events',
  'properties.bootstrap.servers'    = '${EVENTHUBS_BOOTSTRAP_SERVERS}',
  'properties.security.protocol'    = 'SASL_SSL',
  'properties.sasl.mechanism'       = 'PLAIN',
  -- flink-sql-connector-kafka shades kafka-clients under
  -- org.apache.flink.kafka.shaded.*, so JAAS's reflective class lookup needs
  -- that shaded class name -- the unshaded name isn't on the classpath and
  -- fails with "No LoginModule found for org.apache.kafka.common.security.plain.PlainLoginModule".
  'properties.sasl.jaas.config'     = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="${EVENTHUBS_CONNECTION_STRING}";',
  'properties.group.id'             = 'inventory-monitor',
  'scan.startup.mode'               = 'latest-offset',
  'format'                          = 'json',
  'json.timestamp-format.standard'  = 'ISO-8601'
);
