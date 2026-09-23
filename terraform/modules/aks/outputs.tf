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

output "kubelet_identity_object_id" {
  description = "Object ID of the auto-provisioned kubelet identity nodes use to pull container images -- different from cluster_identity_principal_id, which is the control-plane identity"
  value       = azurerm_kubernetes_cluster.this.kubelet_identity[0].object_id
}

output "oidc_issuer_url" {
  description = "This cluster's OIDC token issuer, referenced by azurerm_federated_identity_credential resources to trust ServiceAccount tokens"
  value       = azurerm_kubernetes_cluster.this.oidc_issuer_url
}
