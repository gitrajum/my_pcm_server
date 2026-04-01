# Outputs for Test Environment

output "mcp_server_id" {
  description = "ID of the MCP server container app"
  value       = module.container_app.id
}

output "mcp_server_fqdn" {
  description = "FQDN of the MCP server"
  value       = module.container_app.fqdn
}

output "mcp_server_url" {
  description = "URL of the MCP server"
  value       = module.container_app.url
}

output "latest_revision_name" {
  description = "Name of the latest revision"
  value       = module.container_app.latest_revision_name
}

output "latest_revision_fqdn" {
  description = "FQDN of the latest revision"
  value       = module.container_app.latest_revision_fqdn
}
