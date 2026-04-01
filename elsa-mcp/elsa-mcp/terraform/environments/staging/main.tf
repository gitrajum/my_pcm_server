# Test Environment Configuration

module "container_app" {
  source = "../../modules/container_app"

  # App Configuration
  name                         = "elsa-mcp-staging"
  resource_group_name          = data.azurerm_resource_group.main.name
  location                     = data.azurerm_resource_group.main.location
  container_app_environment_id = data.azurerm_container_app_environment.main.id

  # Container Configuration
  container_image = var.container_image
  container_name  = "elsa-mcp"

  # Port Configuration
  ingress_enabled     = var.ingress_enabled
  ingress_external    = var.ingress_external
  ingress_target_port = var.port
  ingress_transport   = "http"

  # Resource Limits
  cpu    = var.cpu
  memory = var.memory

  # Scaling Configuration
  min_replicas = var.min_replicas
  max_replicas = var.max_replicas

  # Environment Variables and Secrets
  env_vars = var.env_vars
  secrets  = var.secrets

  # Tags
  tags = merge(
    var.tags,
    {
      Environment = "Staging"
      ManagedBy   = "Terraform"
      Service     = "elsa-mcp"
    }
  )
}
