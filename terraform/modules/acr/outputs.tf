output "registry_name" {
  value = azurerm_container_registry.this.name
}

output "login_server" {
  description = "e.g. acrrealtimelakehousedev.azurecr.io -- the prefix used when tagging/pushing images"
  value       = azurerm_container_registry.this.login_server
}
