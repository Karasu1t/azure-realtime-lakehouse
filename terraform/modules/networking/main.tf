resource "azurerm_virtual_network" "this" {
  name                = "vnet-${var.prefix}-dev"
  resource_group_name = var.resource_group_name
  location            = var.location
  address_space       = var.vnet_address_space

  tags = var.tags
}

# AKS nodes (VMs) live in this subnet. Kept separate from other subnets
# so future components (e.g. a bastion) can get their own address range
# without reshaping this one.
resource "azurerm_subnet" "aks" {
  name                 = "snet-aks"
  resource_group_name  = var.resource_group_name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = var.aks_subnet_address_prefix
}
