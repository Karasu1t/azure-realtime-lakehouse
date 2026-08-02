output "namespace_name" {
  value = azurerm_eventhub_namespace.this.name
}

output "kafka_bootstrap_servers" {
  description = "Kafka-compatible bootstrap endpoint, e.g. for Flink's KafkaSource"
  value       = "${azurerm_eventhub_namespace.this.name}.servicebus.windows.net:9093"
}

output "event_hub_name" {
  value = azurerm_eventhub.inventory_events.name
}

output "connection_string" {
  description = "SASL connection string used as the Kafka password (username is always $ConnectionString)"
  value       = azurerm_eventhub_authorization_rule.app.primary_connection_string
  sensitive   = true
}
