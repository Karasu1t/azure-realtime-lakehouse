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

output "primary_access_key" {
  description = "Used as ADLS_ACCOUNT_KEY in k8s/flink-deployment/00_secrets.env and scripts' pyiceberg config -- never write this to a committed file"
  value       = azurerm_storage_account.this.primary_access_key
  sensitive   = true
}
