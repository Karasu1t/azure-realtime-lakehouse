variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "japaneast"
}

variable "prefix" {
  description = "Name prefix shared across resources"
  type        = string
  default     = "realtime-lakehouse"
}
