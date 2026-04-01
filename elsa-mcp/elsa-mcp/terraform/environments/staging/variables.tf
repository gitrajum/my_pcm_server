# Required Variables

variable "resource_group_name" {
  description = "Name of the existing resource group"
  type        = string
}

variable "container_app_environment_name" {
  description = "Name of the existing Container App Environment"
  type        = string
}

# Container Configuration

variable "container_image" {
  description = "Container image for MCP server"
  type        = string
}

variable "port" {
  description = "Port for MCP server"
  type        = number
  default     = 8000
}

# Resource Configuration

variable "cpu" {
  description = "CPU allocation (0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0)"
  type        = number
  default     = 0.5
}

variable "memory" {
  description = "Memory allocation (e.g., '0.5Gi', '1.0Gi', '2.0Gi')"
  type        = string
  default     = "1.0Gi"
}

# Scaling Configuration

variable "min_replicas" {
  description = "Minimum number of replicas"
  type        = number
  default     = 1
}

variable "max_replicas" {
  description = "Maximum number of replicas"
  type        = number
  default     = 3
}

# Ingress Configuration

variable "ingress_enabled" {
  description = "Enable ingress for MCP server"
  type        = bool
  default     = true
}

variable "ingress_external" {
  description = "Make ingress external (public)"
  type        = bool
  default     = false
}

# Environment Variables and Secrets

variable "env_vars" {
  description = "Environment variables for the container"
  type = list(object({
    name  = string
    value = string
  }))
  default = []
}

variable "secrets" {
  description = "Secrets for the container"
  type = list(object({
    name  = string
    value = string
  }))
  default   = []
  sensitive = true
}

# Tags

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
