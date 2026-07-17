output "eventhubs_bootstrap_server" {
  description = "Kafka-protocol bootstrap server for the Event Hubs namespace."
  value       = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
}

output "eventhubs_connection_string" {
  description = "Primary connection string for the send+listen authorization rule (SASL_PASSWORD for Kafka clients)."
  value       = azurerm_eventhub_namespace_authorization_rule.app.primary_connection_string
  sensitive   = true
}

output "acr_login_server" {
  description = "ACR login server, e.g. fraudplacr.azurecr.io."
  value       = azurerm_container_registry.main.login_server
}

output "api_fqdn" {
  description = "Public FQDN of the deployed scoring API Container App."
  value       = azurerm_container_app.api.ingress[0].fqdn
}

output "resource_group_name" {
  description = "Resource group name (used by deploy.sh/destroy.sh for az cli commands)."
  value       = azurerm_resource_group.main.name
}

output "container_app_name" {
  description = "Container App name (used by deploy.sh to update the image post-build)."
  value       = azurerm_container_app.api.name
}
