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

  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  prefix              = var.prefix
  tags                = local.tags
}

module "event_hubs" {
  source = "../../modules/event_hubs"

  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  prefix              = var.prefix
  tags                = local.tags
}
