variable "resource_group_name" {
  description = "Resource group to create the storage account in"
  type        = string
}

variable "location" {
  description = "Azure region for the storage account"
  type        = string
}

variable "prefix" {
  description = "Name prefix shared across resources (hyphens are stripped for the storage account name)"
  type        = string
}

variable "tags" {
  description = "Tags applied to the storage account"
  type        = map(string)
  default     = {}
}
