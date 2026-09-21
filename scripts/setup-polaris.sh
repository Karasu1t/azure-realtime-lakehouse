#!/usr/bin/env bash
set -euo pipefail

# Initialises a freshly started Polaris (in-memory, so this must be re-run
# after every Pod restart): creates the Azure-backed 'lakehouse' catalog and a
# dedicated principal for Flink, and prints that principal's credentials.
# Flink authenticates as this principal instead of the bootstrap root user.
#
# Needs `kubectl port-forward svc/polaris 8181:8181 -n flink` running, and:
#   POLARIS_ROOT_CLIENT_ID / POLARIS_ROOT_CLIENT_SECRET  bootstrap credentials
#   AZURE_TENANT_ID, ADLS_ACCOUNT_NAME                   from terraform output / az

POLARIS_CLI="${POLARIS_CLI:-polaris}"
HOST="${POLARIS_HOST:-localhost}"
PORT="${POLARIS_PORT:-8181}"

root=("$POLARIS_CLI" --host "$HOST" --port "$PORT"
  --client-id "${POLARIS_ROOT_CLIENT_ID:?}" --client-secret "${POLARIS_ROOT_CLIENT_SECRET:?}")

base="abfss://lakehouse@${ADLS_ACCOUNT_NAME:?}.dfs.core.windows.net/"

# default-base-location is the catalog root itself: pointing it at a
# subdirectory makes Polaris reject namespaces as having a custom location.
"${root[@]}" catalogs create --storage-type azure --tenant-id "${AZURE_TENANT_ID:?}" \
  --default-base-location "$base" --allowed-location "$base" lakehouse

"${root[@]}" principal-roles create flink_app_role
"${root[@]}" catalog-roles create --catalog lakehouse lakehouse_writer
"${root[@]}" privileges catalog grant --catalog lakehouse --catalog-role lakehouse_writer CATALOG_MANAGE_CONTENT
"${root[@]}" catalog-roles grant --catalog lakehouse --principal-role flink_app_role lakehouse_writer

# The secret is only returned once, at creation time.
"${root[@]}" principals create flink_app
"${root[@]}" principal-roles grant --principal flink_app flink_app_role
