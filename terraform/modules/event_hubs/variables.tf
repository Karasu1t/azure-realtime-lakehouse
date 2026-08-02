variable "resource_group_name" {
  description = "Resource group to create the namespace in"
  type        = string
}

variable "location" {
  description = "Azure region for the namespace"
  type        = string
}

variable "prefix" {
  description = "Name prefix shared across resources"
  type        = string
}

variable "partition_count" {
  description = "Number of partitions for the inventory-events hub"
  type        = number
  default     = 2
}

variable "message_retention_days" {
  description = "How long events are retained in the hub"
  type        = number
  default     = 1
}

variable "tags" {
  description = "Tags applied to the namespace"
  type        = map(string)
  default     = {}
}
