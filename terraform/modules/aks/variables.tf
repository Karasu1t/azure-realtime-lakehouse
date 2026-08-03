variable "resource_group_name" {
  description = "Resource group to create the cluster in"
  type        = string
}

variable "location" {
  description = "Azure region for the cluster"
  type        = string
}

variable "prefix" {
  description = "Name prefix shared across resources"
  type        = string
}

variable "subnet_id" {
  description = "Subnet the node pool's VMs are attached to"
  type        = string
}

variable "node_count" {
  description = "Number of nodes in the default node pool"
  type        = number
  default     = 2
}

variable "vm_size" {
  description = "VM size for the default node pool"
  type        = string
  # B-series v1 (e.g. Standard_B2s) is not in this subscription's allowed
  # SKU list for japaneast; v2 is.
  default = "Standard_B2s_v2"
}

variable "authorized_ip_ranges" {
  description = "CIDRs allowed to reach the AKS API server (e.g. your home IP as x.x.x.x/32)"
  type        = list(string)
}

variable "tags" {
  description = "Tags applied to the cluster"
  type        = map(string)
  default     = {}
}
