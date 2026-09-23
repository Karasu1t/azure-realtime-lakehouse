#!/usr/bin/env bash
set -euo pipefail

# Fills the ${POLARIS_WORKLOAD_IDENTITY_CLIENT_ID} placeholder in
# 01_serviceaccount.yaml, then applies all of k8s/polaris/ in dependency
# order (namespace and serviceaccount must exist before the Deployment
# that references them). 01_secret.yaml is gitignored -- copy it from
# 02_secret.example.yaml and fill in real values first.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${POLARIS_WORKLOAD_IDENTITY_CLIENT_ID:?set from: terraform output -raw polaris_workload_identity_client_id}"
export POLARIS_WORKLOAD_IDENTITY_CLIENT_ID

kubectl apply -f "${SCRIPT_DIR}/00_namespace.yaml"
envsubst '${POLARIS_WORKLOAD_IDENTITY_CLIENT_ID}' < "${SCRIPT_DIR}/01_serviceaccount.yaml" | kubectl apply -f -
kubectl apply \
  -f "${SCRIPT_DIR}/02_secret.yaml" \
  -f "${SCRIPT_DIR}/03_deployment.yaml" \
  -f "${SCRIPT_DIR}/04_service.yaml"
