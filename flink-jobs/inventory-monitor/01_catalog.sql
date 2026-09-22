-- Iceberg REST catalog backed by Apache Polaris. ${...} tokens are filled in
-- at deploy time (envsubst) from Kubernetes Secrets, never committed as
-- literal values.
CREATE CATALOG polaris_catalog WITH (
  'type'         = 'iceberg',
  'catalog-type' = 'rest',
  'uri'          = 'http://polaris.flink.svc.cluster.local:8181/api/catalog',
  'warehouse'    = 'lakehouse',

  -- 'credential' (not a static 'token') so the Iceberg REST client
  -- fetches and auto-refreshes an OAuth2 token itself via Polaris's
  -- /oauth/tokens endpoint. This job runs indefinitely, and a static
  -- bearer token would eventually expire mid-job with no way to renew it.
  'credential'   = '${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}',
  -- Polaris rejects the Iceberg client's default scope 'catalog' with
  -- invalid_scope; it wants a principal-role scope.
  'scope'        = 'PRINCIPAL_ROLE:ALL',

  -- Iceberg's Azure module talks to ADLS2 directly via abfss:// paths;
  -- Polaris only manages the metadata pointer, not the data files.
  'io-impl'                             = 'org.apache.iceberg.azure.adlsv2.ADLSFileIO',
  'adls.auth.shared-key.account.name'   = '${ADLS_ACCOUNT_NAME}',
  'adls.auth.shared-key.account.key'    = '${ADLS_ACCOUNT_KEY}'
);

-- Deliberately NOT "USE CATALOG polaris_catalog": 02_source.sql's Kafka
-- table isn't Iceberg data and Polaris (an Iceberg-only catalog) rejects
-- it outright ("Creating table with watermark specs is not supported
-- yet"). The Kafka table stays in the default in-memory catalog;
-- 03_sink.sql/04_pipeline.sql reference this one by its full
-- polaris_catalog.inventory.* name instead -- the same cross-catalog
-- INSERT pattern proven in the June kafka-flink-iceberg-handson.
CREATE DATABASE IF NOT EXISTS polaris_catalog.inventory;
