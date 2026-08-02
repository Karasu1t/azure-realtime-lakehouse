locals {
  namespace_name = substr(
    "evh-${replace(lower(var.prefix), "/[^a-z0-9-]/", "")}-dev",
    0, 50
  )
}

resource "azurerm_eventhub_namespace" "this" {
  name                = local.namespace_name
  resource_group_name = var.resource_group_name
  location            = var.location

  # Standard tier is the minimum that exposes the Kafka-compatible endpoint;
  # Flink connects to it with the plain Kafka source connector.
  sku      = "Standard"
  capacity = 1

  tags = var.tags
}

resource "azurerm_eventhub" "inventory_events" {
  name              = "inventory-events"
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = var.partition_count
  message_retention = var.message_retention_days
}

# Send/Listen policy scoped to this hub only, used by the simulator (produce)
# and the Flink job (consume) via SASL_SSL / Kafka protocol.
resource "azurerm_eventhub_authorization_rule" "app" {
  name                = "flink-app"
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = azurerm_eventhub.inventory_events.name
  resource_group_name = var.resource_group_name

  listen = true
  send   = true
  manage = false
}
