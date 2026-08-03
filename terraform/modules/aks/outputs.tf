output "cluster_name" {
  value = azurerm_kubernetes_cluster.this.name
}

output "kube_config" {
  value     = azurerm_kubernetes_cluster.this.kube_config_raw
  sensitive = true
}

output "cluster_identity_principal_id" {
  description = "Object ID of the cluster's system-assigned identity, used for RBAC role assignments (e.g. ADLS2 access)"
  value       = azurerm_kubernetes_cluster.this.identity[0].principal_id
}
