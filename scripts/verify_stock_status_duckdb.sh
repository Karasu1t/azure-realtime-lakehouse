#!/usr/bin/env bash
set -euo pipefail

# Alternative to verify_stock_status.py: pyiceberg 0.12.0 can't merge
# equality-delete files (see README ADR -- 03_sink.sql's
# write.upsert.enabled=true produces them on every product_id update),
# so table.scan() fails there. DuckDB's iceberg+azure extensions handle
# equality deletes and can read the actual row data, no Flink/sink
# redesign needed.
#
# Same port-forward requirement as verify_stock_status.py:
#   kubectl port-forward svc/polaris 8181:8181 -n flink
#
# Needs: POLARIS_CLIENT_ID, POLARIS_CLIENT_SECRET, ADLS_ACCOUNT_NAME,
# ADLS_ACCOUNT_KEY (same as verify_stock_status.py / catalog.py).

: "${POLARIS_CLIENT_ID:?}" "${POLARIS_CLIENT_SECRET:?}" "${ADLS_ACCOUNT_NAME:?}" "${ADLS_ACCOUNT_KEY:?}"

duckdb -c "
INSTALL iceberg; LOAD iceberg;
INSTALL azure; LOAD azure;

-- Default Azure SDK transport failed with 'Problem with the SSL CA cert'
-- even though the system CA bundle (/etc/ssl/certs/ca-certificates.crt)
-- exists and curl itself works fine against the same endpoint -- the
-- default adapter just doesn't find it. Switching to the curl-based
-- transport adapter (which does use the system bundle) fixed it.
SET azure_transport_option_type = 'curl';

CREATE SECRET polaris_oauth (
  TYPE ICEBERG,
  CLIENT_ID '${POLARIS_CLIENT_ID}',
  CLIENT_SECRET '${POLARIS_CLIENT_SECRET}',
  OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL',
  OAUTH2_SERVER_URI 'http://localhost:8181/api/catalog/v1/oauth/tokens'
);

CREATE SECRET adls_key (
  TYPE AZURE,
  CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=${ADLS_ACCOUNT_NAME};AccountKey=${ADLS_ACCOUNT_KEY};EndpointSuffix=core.windows.net'
);

ATTACH 'lakehouse' AS lakehouse (
  TYPE ICEBERG,
  ENDPOINT 'http://localhost:8181/api/catalog',
  SECRET polaris_oauth
);

SELECT * FROM lakehouse.inventory.stock_status ORDER BY product_id;
"
