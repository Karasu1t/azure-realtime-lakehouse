locals {
  registry_name = substr(
    "acr${replace(lower(var.prefix), "/[^a-z0-9]/", "")}dev",
    0, 50
  )
}

resource "azurerm_container_registry" "this" {
  name                = local.registry_name
  resource_group_name = var.resource_group_name
  location            = var.location

  # Basic: cheapest tier, no georeplication or private endpoints -- fine
  # for a single small image pulled by one cluster in one region.
  sku           = "Basic"
  admin_enabled = false

  tags = var.tags
}

# AcrPull lets AKS nodes pull images with their own managed identity --
# no image pull secret to create, store, or rotate.
resource "azurerm_role_assignment" "aks_acr_pull" {
  scope                = azurerm_container_registry.this.id
  role_definition_name = "AcrPull"
  principal_id         = var.aks_kubelet_identity_principal_id
}
