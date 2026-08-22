#!/usr/bin/env bash
set -euo pipefail

# Builds the SQL runner jar and pushes the image AKS will pull for
# spec.image in k8s/flink-deployment/02_flinkdeployment.yaml.
#
# NOT YET WIRED UP: this assumes an Azure Container Registry already
# exists and that the AKS cluster has been granted AcrPull on it. Neither
# exists in terraform/modules yet -- there's no azurerm_container_registry
# resource, and no role assignment granting the AKS cluster's identity
# pull access. That's the next real gap to close, not this script.

ACR_NAME="${ACR_NAME:?set ACR_NAME to the Azure Container Registry name, e.g. acrrealtimelakehousedev}"
IMAGE="${ACR_NAME}.azurecr.io/sql-runner:latest"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

mvn -q clean package
docker build -t "${IMAGE}" .

az acr login --name "${ACR_NAME}"
docker push "${IMAGE}"

echo "Built and pushed ${IMAGE}"
echo "Update spec.image in k8s/flink-deployment/02_flinkdeployment.yaml to match."
