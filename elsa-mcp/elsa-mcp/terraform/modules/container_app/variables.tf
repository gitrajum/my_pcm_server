variable "name" {
  description = "Name of the container app"
  type        = string
}

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "container_app_environment_id" {
  description = "ID of the Container App Environment"
  type        = string
}

variable "container_image" {
  description = "Container image to deploy"
  type        = string
}

variable "container_name" {
  description = "Name of the container"
  type        = string
}

variable "cpu" {
  description = "CPU allocation"
  type        = number
  default     = 0.5
}

variable "memory" {
  description = "Memory allocation"
  type        = string
  default     = "1.0Gi"
}

variable "min_replicas" {
  description = "Minimum replicas"
  type        = number
  default     = 1
}

variable "max_replicas" {
  description = "Maximum replicas"
  type        = number
  default     = 3
}

variable "ingress_enabled" {
  description = "Enable ingress"
  type        = bool
  default     = false
}

variable "ingress_external" {
  description = "External ingress"
  type        = bool
  default     = false
}

variable "ingress_target_port" {
  description = "Target port for ingress"
  type        = number
  default     = 80
}

variable "ingress_transport" {
  description = "Transport protocol"
  type        = string
  default     = "http"
}

variable "env_vars" {
  description = "Environment variables"
  type = list(object({
    name  = string
    value = string
  }))
  default = []
}

variable "secrets" {
  description = "Secrets"
  type = list(object({
    name  = string
    value = string
  }))
  default   = []
  sensitive = true
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
