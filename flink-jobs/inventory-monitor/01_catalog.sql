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

  -- Iceberg's Azure module talks to ADLS2 directly via abfss:// paths;
  -- Polaris only manages the metadata pointer, not the data files.
  'io-impl'                             = 'org.apache.iceberg.azure.adlsv2.ADLSFileIO',
  'adls.auth.shared-key.account.name'   = '${ADLS_ACCOUNT_NAME}',
  'adls.auth.shared-key.account.key'    = '${ADLS_ACCOUNT_KEY}'
);

USE CATALOG polaris_catalog;

CREATE DATABASE IF NOT EXISTS inventory;
