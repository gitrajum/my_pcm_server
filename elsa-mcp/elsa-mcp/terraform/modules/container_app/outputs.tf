output "id" {
  description = "ID of the container app"
  value       = azurerm_container_app.app.id
}

output "fqdn" {
  description = "FQDN of the container app"
  value       = var.ingress_enabled ? azurerm_container_app.app.ingress[0].fqdn : null
}

output "url" {
  description = "URL of the container app"
  value       = var.ingress_enabled ? "https://${azurerm_container_app.app.ingress[0].fqdn}" : null
}

output "name" {
  description = "Name of the container app"
  value       = azurerm_container_app.app.name
}
