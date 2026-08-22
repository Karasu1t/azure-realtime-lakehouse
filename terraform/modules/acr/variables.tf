variable "resource_group_name" {
  description = "Resource group to create the registry in"
  type        = string
}

variable "location" {
  description = "Azure region for the registry"
  type        = string
}

variable "prefix" {
  description = "Name prefix shared across resources"
  type        = string
}

variable "aks_kubelet_identity_principal_id" {
  description = "Principal ID of the AKS cluster's kubelet identity, granted AcrPull on this registry so nodes can pull images without a stored credential"
  type        = string
}

variable "tags" {
  description = "Tags applied to the registry"
  type        = map(string)
  default     = {}
}
