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
