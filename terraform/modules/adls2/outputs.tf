output "storage_account_name" {
  value = azurerm_storage_account.this.name
}

output "storage_account_id" {
  value = azurerm_storage_account.this.id
}

output "primary_dfs_endpoint" {
  description = "ADLS Gen2 (dfs) endpoint, used by Flink/Iceberg for abfss:// paths"
  value       = azurerm_storage_account.this.primary_dfs_endpoint
}

output "lakehouse_container_name" {
  value = azurerm_storage_container.lakehouse.name
}
