#!/usr/bin/env bash
set -euo pipefail

# Builds the SQL runner jar and pushes the image to the ACR provisioned
# by terraform/modules/acr (AKS's kubelet identity already has AcrPull on
# it, so no image pull secret is needed).
#
# ACR_NAME is the registry name terraform created, e.g.
# `terraform output -raw acr_login_server` in terraform/env/dev, minus
# the .azurecr.io suffix.

ACR_NAME="${ACR_NAME:?set ACR_NAME to the Azure Container Registry name, e.g. acrrealtimelakehousedev}"
TAG="${SQL_RUNNER_TAG:?set SQL_RUNNER_TAG, e.g. the short git commit hash; do not reuse a tag (nodes cache by tag)}"
IMAGE="${ACR_NAME}.azurecr.io/sql-runner:${TAG}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

mvn -q clean package
docker build -t "${IMAGE}" .

az acr login --name "${ACR_NAME}"
docker push "${IMAGE}"

echo "Built and pushed ${IMAGE}"
echo "Set ACR_LOGIN_SERVER=${ACR_NAME}.azurecr.io and SQL_RUNNER_TAG=${TAG} in k8s/flink-deployment/00_secrets.env"
