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

# Polaris validates the Iceberg table's storage location during CREATE TABLE
# using its own Azure identity, not Flink's shared-key credential -- it runs
# with no explicit Azure credential configured, so its DefaultAzureCredential
# chain resolves to whatever identity the AKS node itself carries via IMDS,
# which is this same kubelet identity (already used for AcrPull above).
# Production would replace this with Workload Identity federated to a
# dedicated identity instead of reusing the node's kubelet identity.
#
# Scope is the whole storage account, not just the lakehouse container:
# a raw DFS REST call with a plain OAuth token succeeded against the
# container-scoped assignment, but Polaris kept failing with the same
# identity -- Polaris likely requests a User Delegation Key to vend/validate
# storage access, and generateUserDelegationKey is an account-level action
# that a container-scoped role assignment cannot grant.
resource "azurerm_role_assignment" "aks_storage_blob_contributor" {
  scope                = module.adls2.storage_account_id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = module.aks.kubelet_identity_object_id
}
