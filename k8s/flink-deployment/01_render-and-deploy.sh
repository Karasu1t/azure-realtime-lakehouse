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
       ADLS_ACCOUNT_NAME ADLS_ACCOUNT_KEY POLARIS_ACCESS_TOKEN LOW_STOCK_THRESHOLD

for f in "${SQL_DIR}"/*.sql; do
  envsubst < "$f" > "${RENDERED_DIR}/$(basename "$f")"
done

kubectl create configmap flink-sql \
  --namespace flink \
  --from-file="${RENDERED_DIR}" \
  --dry-run=client -o yaml | kubectl apply -f -

envsubst < "${SCRIPT_DIR}/02_flinkdeployment.yaml" | kubectl apply -f -
kubectl wait --for=condition=Ready flinkdeployment/inventory-monitor \
  --namespace flink --timeout=180s
