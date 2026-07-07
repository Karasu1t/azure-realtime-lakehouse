resource "azurerm_resource_group" "main" {
  name     = "rg-${var.prefix}-dev"
  location = var.location

  tags = {
    project    = "azure-realtime-lakehouse"
    env        = "dev"
    managed_by = "terraform"
  }
}
