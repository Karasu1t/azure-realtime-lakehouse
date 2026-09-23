locals {
  tags = {
    project    = "azure-realtime-lakehouse"
    env        = "dev"
    managed_by = "terraform"
  }
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${var.prefix}-dev"
  location = var.location

  tags = local.tags
}

module "adls2" {
  source = "../../modules/adls2"

  resource_group_name  = azurerm_resource_group.main.name
  location             = azurerm_resource_group.main.location
  prefix               = var.prefix
  allowed_subnet_ids   = [module.networking.aks_subnet_id]
  allowed_ip_addresses = [for cidr in var.authorized_ip_ranges : trimsuffix(cidr, "/32")]
  tags                 = local.tags
}

module "event_hubs" {
  source = "../../modules/event_hubs"

  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  prefix              = var.prefix
  tags                = local.tags
}

module "networking" {
  source = "../../modules/networking"

  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  prefix              = var.prefix
  tags                = local.tags
}

module "aks" {
  source = "../../modules/aks"

  resource_group_name  = azurerm_resource_group.main.name
  location             = azurerm_resource_group.main.location
  prefix               = var.prefix
  subnet_id            = module.networking.aks_subnet_id
  authorized_ip_ranges = var.authorized_ip_ranges
  tags                 = local.tags
}

module "acr" {
  source = "../../modules/acr"

  resource_group_name               = azurerm_resource_group.main.name
  location                          = azurerm_resource_group.main.location
  prefix                            = var.prefix
  aks_kubelet_identity_principal_id = module.aks.kubelet_identity_object_id
  tags                              = local.tags
}

# --- Workload Identity for Flink and Polaris ---
#
# Originally both Flink (shared-key in 01_catalog.sql) and Polaris
# (implicitly, via the AKS node's kubelet identity over IMDS -- discovered
# the hard way: Polaris validates a table's storage location during
# CREATE TABLE using its own Azure identity, and with no credential
# configured, DefaultAzureCredential fell back to whatever identity the
# node carries) reached ADLS2 through mechanisms that either leak a
# long-lived key or over-grant the node's own identity. Workload Identity
# lets each Pod present its own dedicated identity instead, via a
# ServiceAccount token federated to Azure AD -- no key, and the node's
# kubelet identity goes back to only needing AcrPull.
#
# Scope is the whole storage account, not just the lakehouse container:
# a raw DFS REST call with a plain OAuth token succeeded against a
# container-scoped assignment, but Polaris kept failing with the same
# identity -- Polaris likely requests a User Delegation Key to vend/validate
# storage access, and generateUserDelegationKey is an account-level action
# that a container-scoped role assignment cannot grant. Flink doesn't need
# that, but using the same scope for both keeps this simple.

resource "azurerm_user_assigned_identity" "flink" {
  name                = "id-flink-${var.prefix}-dev"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = local.tags
}

# subject must match the ServiceAccount FlinkDeployment.spec.serviceAccount
# actually runs as (system:serviceaccount:<namespace>:<name>) -- see
# k8s/flink-deployment/00_serviceaccount.yaml.
resource "azurerm_federated_identity_credential" "flink" {
  name                      = "flink-workload-identity"
  user_assigned_identity_id = azurerm_user_assigned_identity.flink.id
  audience                  = ["api://AzureADTokenExchange"]
  issuer                    = module.aks.oidc_issuer_url
  subject                   = "system:serviceaccount:flink:flink"
}

resource "azurerm_role_assignment" "flink_storage" {
  scope                = module.adls2.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.flink.principal_id
}

resource "azurerm_user_assigned_identity" "polaris" {
  name                = "id-polaris-${var.prefix}-dev"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = local.tags
}

resource "azurerm_federated_identity_credential" "polaris" {
  name                      = "polaris-workload-identity"
  user_assigned_identity_id = azurerm_user_assigned_identity.polaris.id
  audience                  = ["api://AzureADTokenExchange"]
  issuer                    = module.aks.oidc_issuer_url
  subject                   = "system:serviceaccount:flink:polaris"
}

resource "azurerm_role_assignment" "polaris_storage" {
  scope                = module.adls2.storage_account_id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = azurerm_user_assigned_identity.polaris.principal_id
}
