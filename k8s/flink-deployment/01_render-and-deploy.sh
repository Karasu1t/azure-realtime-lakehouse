#!/usr/bin/env bash
set -euo pipefail

# Fills the ${VAR} placeholders in flink-jobs/inventory-monitor/*.sql from
# 00_secrets.env, packs the rendered files into a ConfigMap, then applies
# the FlinkDeployment. Rendering happens here instead of committing a
# pre-filled ConfigMap YAML, so the SQL files stay the single source of
# truth and no secret ever touches the repo.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="${SCRIPT_DIR}/../../flink-jobs/inventory-monitor"
RENDERED_DIR="$(mktemp -d)"
trap 'rm -rf "${RENDERED_DIR}"' EXIT

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/00_secrets.env"
export EVENTHUBS_BOOTSTRAP_SERVERS EVENTHUBS_CONNECTION_STRING \
       ADLS_ACCOUNT_NAME ADLS_ACCOUNT_KEY \
       POLARIS_CLIENT_ID POLARIS_CLIENT_SECRET \
       ACR_LOGIN_SERVER SQL_RUNNER_TAG LOW_STOCK_THRESHOLD \
       FLINK_WORKLOAD_IDENTITY_CLIENT_ID

# The variable list passed to envsubst matters: without it, envsubst
# substitutes *every* $VAR-shaped token in the file, including the
# unrelated literal "$ConnectionString" that Event Hubs' Kafka SASL
# convention requires as-is (02_source.sql) -- envsubst silently replaced
# it with an empty string since no env var named ConnectionString exists,
# breaking authentication with no error until Flink's SQL parser choked
# on the resulting empty-quoted string.
ENVSUBST_VARS='${EVENTHUBS_BOOTSTRAP_SERVERS} ${EVENTHUBS_CONNECTION_STRING} ${ADLS_ACCOUNT_NAME} ${ADLS_ACCOUNT_KEY} ${POLARIS_CLIENT_ID} ${POLARIS_CLIENT_SECRET} ${ACR_LOGIN_SERVER} ${SQL_RUNNER_TAG} ${LOW_STOCK_THRESHOLD} ${FLINK_WORKLOAD_IDENTITY_CLIENT_ID}'

for f in "${SQL_DIR}"/*.sql; do
  envsubst "${ENVSUBST_VARS}" < "$f" > "${RENDERED_DIR}/$(basename "$f")"
done

kubectl create configmap flink-sql \
  --namespace flink \
  --from-file="${RENDERED_DIR}" \
  --dry-run=client -o yaml | kubectl apply -f -

# ServiceAccount first: the FlinkDeployment Pod references it by name and
# won't schedule if it doesn't exist yet.
envsubst "${ENVSUBST_VARS}" < "${SCRIPT_DIR}/00_serviceaccount.yaml" | kubectl apply -f -
envsubst "${ENVSUBST_VARS}" < "${SCRIPT_DIR}/02_flinkdeployment.yaml" | kubectl apply -f -
kubectl wait --for=condition=Ready flinkdeployment/inventory-monitor \
  --namespace flink --timeout=180s
