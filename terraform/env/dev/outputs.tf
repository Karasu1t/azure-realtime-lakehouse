# Surfaces the values needed to populate k8s/flink-deployment/00_secrets.env
# and scripts' pyiceberg config after `terraform apply`, e.g.:
#   terraform output -raw adls_account_key

output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "adls_account_name" {
  value = module.adls2.storage_account_name
}

output "adls_account_key" {
  value     = module.adls2.primary_access_key
  sensitive = true
}

output "eventhubs_bootstrap_servers" {
  value = module.event_hubs.kafka_bootstrap_servers
}

output "eventhubs_connection_string" {
  value     = module.event_hubs.connection_string
  sensitive = true
}

output "acr_login_server" {
  value = module.acr.login_server
}

output "aks_cluster_name" {
  value = module.aks.cluster_name
}
