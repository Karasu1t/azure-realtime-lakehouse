resource "azurerm_kubernetes_cluster" "this" {
  name                = "aks-${var.prefix}-dev"
  resource_group_name = var.resource_group_name
  location            = var.location
  dns_prefix          = "${var.prefix}-dev"

  # Free tier: no SLA on the control plane, no cost for it either.
  # Fine for a portfolio cluster that gets stopped/destroyed between sessions.
  sku_tier = "Free"

  default_node_pool {
    name           = "system"
    node_count     = var.node_count
    vm_size        = var.vm_size
    vnet_subnet_id = var.subnet_id
  }

  # Cluster authenticates to other Azure resources (e.g. ADLS2) as itself,
  # instead of a client secret that would need to be stored somewhere.
  identity {
    type = "SystemAssigned"
  }

  # Lets individual Pods (via a matching ServiceAccount + Federated
  # Credential) present their own Azure identity, instead of every Pod on
  # the node sharing the node's kubelet identity. oidc_issuer_enabled
  # stands up the OIDC token issuer Workload Identity federation checks
  # against; workload_identity_enabled installs the mutating webhook that
  # injects the token/env vars into annotated Pods.
  oidc_issuer_enabled      = true
  workload_identity_enabled = true

  # API server stays publicly reachable but locked down to specific IPs
  # (e.g. home IP) rather than requiring a private network / bastion setup.
  api_server_access_profile {
    authorized_ip_ranges = var.authorized_ip_ranges
  }

  network_profile {
    network_plugin = "azure"
  }

  tags = var.tags
}
