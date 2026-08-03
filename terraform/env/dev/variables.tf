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

variable "authorized_ip_ranges" {
  description = "CIDRs allowed to reach the AKS API server, e.g. [\"x.x.x.x/32\"] for your home IP. Set via dev.tfvars (gitignored)."
  type        = list(string)
}
