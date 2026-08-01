locals {
  # Storage account names must be 3-24 chars, lowercase letters and digits only.
  storage_account_name = substr(
    "st${replace(lower(var.prefix), "/[^a-z0-9]/", "")}dev",
    0, 24
  )
}

resource "azurerm_storage_account" "this" {
  name                = local.storage_account_name
  resource_group_name = var.resource_group_name
  location            = var.location

  account_tier             = "Standard"
  account_replication_type = "LRS"

  # Hierarchical namespace turns this Blob-backed account into ADLS Gen2:
  # directories become real objects instead of "/"-delimited key prefixes,
  # which is what lets Iceberg/Flink do efficient directory-level operations.
  is_hns_enabled = true

  min_tls_version                 = "TLS1_2"
  public_network_access_enabled   = false
  allow_nested_items_to_be_public = false

  tags = var.tags
}

resource "azurerm_storage_container" "lakehouse" {
  name                  = "lakehouse"
  storage_account_id    = azurerm_storage_account.this.id
  container_access_type = "private"
}
