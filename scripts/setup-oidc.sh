#!/usr/bin/env bash
set -euo pipefail

# One-time, human-run bootstrap: creates the Azure AD App Registration
# (Service Principal) and Federated Credential that let GitHub Actions
# authenticate via OIDC -- no client secret is ever created or stored.
# Same "run once outside Terraform" category as rg-tfstate: this SP is
# what CI uses to run terraform apply/destroy, so it can't be provisioned
# by that same terraform run.
#
# Role scope is the whole subscription, not the resource group, because
# this SP has to be able to create rg-realtime-lakehouse-dev itself --
# resource group creation is a subscription-level permission. A tighter
# scope isn't possible until the RG already exists, which defeats the
# point.

REPO="Karasu1t/azure-realtime-lakehouse"
APP_NAME="gh-actions-${REPO##*/}"
SUBSCRIPTION_ID="${ARM_SUBSCRIPTION_ID:?set ARM_SUBSCRIPTION_ID}"

APP_ID=$(az ad app create --display-name "${APP_NAME}" --query appId -o tsv)
az ad sp create --id "${APP_ID}" >/dev/null

az role assignment create \
  --assignee "${APP_ID}" \
  --role "Contributor" \
  --scope "/subscriptions/${SUBSCRIPTION_ID}"

# Trusts only workflow runs against the main branch -- these workflows
# can create/destroy real cloud infra, so trust isn't extended to PRs or
# other branches.
az ad app federated-credential create \
  --id "${APP_ID}" \
  --parameters "{
    \"name\": \"github-actions-main\",
    \"issuer\": \"https://token.actions.githubusercontent.com\",
    \"subject\": \"repo:${REPO}:ref:refs/heads/main\",
    \"audiences\": [\"api://AzureADTokenExchange\"]
  }"

TENANT_ID=$(az account show --query tenantId -o tsv)

echo
echo "Set these as GitHub Actions repository *variables* (Settings > Secrets and variables > Actions > Variables):"
echo "  AZURE_CLIENT_ID       = ${APP_ID}"
echo "  AZURE_TENANT_ID       = ${TENANT_ID}"
echo "  AZURE_SUBSCRIPTION_ID = ${SUBSCRIPTION_ID}"
echo "None of these need to be secrets -- OIDC trust is what grants access, not knowledge of these IDs."
