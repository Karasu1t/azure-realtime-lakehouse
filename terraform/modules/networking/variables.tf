variable "resource_group_name" {
  description = "Resource group to create the network in"
  type        = string
}

variable "location" {
  description = "Azure region for the network"
  type        = string
}

variable "prefix" {
  description = "Name prefix shared across resources"
  type        = string
}

variable "vnet_address_space" {
  description = "Address space for the VNet"
  type        = list(string)
  default     = ["10.10.0.0/16"]
}

variable "aks_subnet_address_prefix" {
  description = "Subnet CIDR for AKS nodes"
  type        = list(string)
  default     = ["10.10.1.0/24"]
}

variable "tags" {
  description = "Tags applied to networking resources"
  type        = map(string)
  default     = {}
}
