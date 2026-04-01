# ElsaMcp - Terraform Infrastructure

Infrastructure as Code for deploying the ElsaMcp MCP server to Azure.

## 📁 Structure

```
terraform/
├── environments/          # Environment-specific configurations
│   ├── test/             # Test/development environment
│   ├── staging/          # Staging environment
│   └── prod/             # Production environment
└── modules/              # Reusable Terraform modules
    └── container_app/    # Azure Container App module
```

## 🏗️ Architecture

This infrastructure deploys the MCP server as an Azure Container App. It **references existing infrastructure** created by the use case:

- **Resource Group** - Provided by use case infrastructure
- **Container App Environment** - Provided by use case infrastructure  
- **Container Registry** - Provided by use case infrastructure
- **Networking** - Provided by use case infrastructure

This MCP server infrastructure **only creates**:
- Azure Container App for the MCP server
- Container configuration (CPU, memory, scaling)
- Ingress configuration
- Environment variables and secrets

## 🚀 Usage

### Prerequisites

1. **Use case infrastructure deployed** - The MCP server requires existing infrastructure:
   ```bash
   # Deploy use case infrastructure first
   cd ../../agentic_ai_use_cases/<your-usecase>/
   agenticai infra apply <usecase> --env test
   ```

2. **Container image built and pushed**:
   ```bash
   elsa-mcp-cli docker build
   elsa-mcp-cli docker push
   ```

3. **Terraform state storage** - Azure Storage Account for state

### Environment Configuration

Each environment has its own directory with configuration files:

```bash
environments/<env>/
├── providers.tf       # Terraform and provider configuration
├── data.tf           # Reference to existing infrastructure
├── main.tf           # Container app deployment
├── variables.tf      # Input variables
├── outputs.tf        # Output values
└── terraform.tfvars  # Variable values (customize this!)
```

### Deployment Steps

#### 1. Configure Variables

Edit `environments/<env>/terraform.tfvars`:

```hcl
# Required: Existing infrastructure references
resource_group_name            = "rg-your-usecase-test"
container_app_environment_name = "cae-your-usecase-test"

# Container configuration
container_image = "yourregistry.azurecr.io/elsa-mcp:latest"

# Resource limits
cpu    = 0.5
memory = "1.0Gi"

# Scaling
min_replicas = 1
max_replicas = 3

# Ingress
ingress_enabled  = true
ingress_external = false  # Internal by default

# Environment variables (customize as needed)
env_vars = [
  {
    name  = "LOG_LEVEL"
    value = "info"
  }
]

# Tags
tags = {
  Project     = "your-project"
  Environment = "test"
  ManagedBy   = "Terraform"
}
```

#### 2. Initialize Terraform

```bash
# Using CLI (recommended)
elsa-mcp-cli infra init --env test \
  --state-rg <state-resource-group> \
  --state-storage <state-storage-account>

# Or manually
cd environments/test
terraform init \
  -backend-config="resource_group_name=<state-rg>" \
  -backend-config="storage_account_name=<state-storage>" \
  -backend-config="container_name=tfstate" \
  -backend-config="key=elsa-mcp-test.tfstate"
```

#### 3. Validate Configuration

```bash
# Using CLI
elsa-mcp-cli infra validate --env test

# Or manually
cd environments/test
terraform validate
```

#### 4. Plan Deployment

```bash
# Using CLI
elsa-mcp-cli infra plan --env test \
  -i yourregistry.azurecr.io/elsa-mcp:latest \
  --state-rg <state-rg> \
  --state-storage <state-storage>

# Or manually
cd environments/test
terraform plan
```

#### 5. Apply Changes

```bash
# Using CLI (recommended)
elsa-mcp-cli infra apply --env test \
  -i yourregistry.azurecr.io/elsa-mcp:latest \
  --state-rg <state-rg> \
  --state-storage <state-storage>

# Or manually
cd environments/test
terraform apply
```

#### 6. View Outputs

```bash
# Using CLI
elsa-mcp-cli infra output --env test

# Or manually
cd environments/test
terraform output
```

## 📊 Outputs

After deployment, the following outputs are available:

- `mcp_server_id` - Azure resource ID of the container app
- `mcp_server_fqdn` - Fully qualified domain name
- `mcp_server_url` - Complete URL (if ingress enabled)
- `latest_revision_name` - Current revision name
- `latest_revision_fqdn` - Current revision FQDN

## 🔧 Configuration Options

### Resource Limits

| CPU | Memory | Use Case |
|-----|--------|----------|
| 0.25 | 0.5Gi | Minimal, infrequent use |
| 0.5 | 1.0Gi | **Default** - Light workloads |
| 1.0 | 2.0Gi | Moderate workloads |
| 2.0 | 4.0Gi | Heavy workloads |

### Scaling

```hcl
min_replicas = 1  # Minimum instances (0 = scale to zero)
max_replicas = 3  # Maximum instances for auto-scaling
```

### Ingress

```hcl
ingress_enabled  = true   # Enable HTTP ingress
ingress_external = false  # false = internal (VNet), true = public
```

## 🌍 Environments

### Test Environment
- **Purpose**: Development and testing
- **Scaling**: Minimal (1 replica)
- **Resources**: Low (0.5 CPU, 1Gi memory)
- **Ingress**: Internal only
- **Cost**: ~$15-30/month

### Staging Environment
- **Purpose**: Pre-production validation
- **Scaling**: Moderate (1-3 replicas)
- **Resources**: Moderate (1.0 CPU, 2Gi memory)
- **Ingress**: Internal only
- **Cost**: ~$30-60/month

### Production Environment
- **Purpose**: Live production workloads
- **Scaling**: Auto-scale (1-5 replicas)
- **Resources**: Adequate (1.0+ CPU, 2Gi+ memory)
- **Ingress**: As required
- **Cost**: ~$50-200/month (depends on usage)

## 🔐 Security

### Managed Identity

The container app uses System-Assigned Managed Identity for secure authentication to Azure services:

```hcl
identity {
  type = "SystemAssigned"
}
```

### Secrets

Store sensitive values in Azure Key Vault and reference them:

```hcl
secrets = [
  {
    name  = "api-key"
    value = "@Microsoft.KeyVault(SecretUri=https://...)"
  }
]
```

### Network Isolation

- Internal ingress by default (not publicly accessible)
- Deployed within VNet (configured in use case infrastructure)
- Service-to-service communication within VNet

## 🔄 Updates and Rollbacks

### Update Container Image

```bash
# Build new image
elsa-mcp-cli docker build

# Push new image
elsa-mcp-cli docker push

# Deploy new version
elsa-mcp-cli infra apply --env test \
  -i yourregistry.azurecr.io/elsa-mcp:v1.2.0 \
  --state-rg <state-rg> \
  --state-storage <state-storage>
```

### Rollback

Container Apps support revision-based rollbacks:

```bash
# List revisions
az containerapp revision list \
  --name elsa-mcp-test \
  --resource-group <rg-name>

# Activate previous revision
az containerapp revision activate \
  --name elsa-mcp-test \
  --resource-group <rg-name> \
  --revision <previous-revision-name>
```

## 🧹 Cleanup

### Destroy Infrastructure

```bash
# Using CLI
elsa-mcp-cli infra destroy --env test \
  --state-rg <state-rg> \
  --state-storage <state-storage>

# Or manually
cd environments/test
terraform destroy
```

⚠️ **Warning**: This only destroys the MCP server container app, not the shared use case infrastructure.

## 📝 Module Reference

### Container App Module

Located in `modules/container_app/`, this module creates an Azure Container App with:

**Inputs:**
- `name` - Container app name
- `resource_group_name` - Target resource group
- `location` - Azure region
- `container_app_environment_id` - Container Apps Environment ID
- `container_image` - Docker image
- `container_name` - Container name
- `cpu` - CPU allocation
- `memory` - Memory allocation
- `min_replicas` - Minimum replicas
- `max_replicas` - Maximum replicas
- `ingress_enabled` - Enable ingress
- `ingress_external` - External vs internal
- `ingress_target_port` - Target port
- `env_vars` - Environment variables
- `secrets` - Secret values

**Outputs:**
- `id` - Container app ID
- `fqdn` - Fully qualified domain name
- `url` - Complete URL
- `latest_revision_name` - Current revision
- `latest_revision_fqdn` - Current revision FQDN

## 🆘 Troubleshooting

### Container App Not Starting

```bash
# Check container logs
az containerapp logs show \
  --name elsa-mcp-test \
  --resource-group <rg-name> \
  --follow

# Check container app details
az containerapp show \
  --name elsa-mcp-test \
  --resource-group <rg-name>
```

### Terraform State Issues

```bash
# Refresh state
cd environments/test
terraform refresh

# Re-initialize
terraform init -reconfigure
```

### Cannot Find Resource Group

Ensure use case infrastructure is deployed first:

```bash
# Check if resource group exists
az group show --name <rg-name>

# Deploy use case infrastructure
cd ../../agentic_ai_use_cases/<usecase>/
agenticai infra apply <usecase> --env test
```

## 📚 Additional Resources

- [Azure Container Apps Documentation](https://docs.microsoft.com/azure/container-apps/)
- [Terraform Azure Provider](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs)
- [AgenticAI CLI Documentation](../../CLI_REFERENCE.md)
- [Golden Path Guide](../../GOLDEN_PATH.md)

---

*Last updated: December 18, 2025*
