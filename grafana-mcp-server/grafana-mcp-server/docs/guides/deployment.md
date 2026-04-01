# Deployment Guide

This guide covers deploying GrafanaMcpServer to Azure Container Apps using the included Terraform infrastructure and CLI tools.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Deployment](#detailed-deployment)
- [Configuration](#configuration)
- [CLI Reference](#cli-reference)
- [Troubleshooting](#troubleshooting)
- [Advanced Topics](#advanced-topics)

## Prerequisites

### Required Tools

- **Azure CLI**: Install from https://docs.microsoft.com/en-us/cli/azure/install-azure-cli
- **Terraform**: Version 1.5 or higher from https://www.terraform.io/downloads
- **Docker**: For building container images from https://www.docker.com/products/docker-desktop
- **Python 3.11+**: For running the CLI tool

### Azure Requirements

- Azure subscription with appropriate permissions
- Resource Groups creation permission
- Container Apps and Container Registry creation permission

### Authentication

```bash
# Login to Azure
az login

# Set subscription (if you have multiple)
az account set --subscription <subscription-id>

# Verify authentication
az account show
```

## Quick Start

### 1. Install CLI Tool

```bash
# Install with CLI dependencies
pip install -e ".[cli]"

# Verify installation
grafana-mcp-server-cli --version
```

### 2. Configure Deployment

```bash
# Copy example configuration
cp terraform/terraform.tfvars.example terraform/terraform.tfvars

# Edit with your values
nano terraform/terraform.tfvars
```

Minimum required configuration:

```hcl
subscription_id = "your-subscription-id"
resource_group_name = "rg-grafana-mcp-server-dev"
location = "eastus"
mcp_container_image = "your-registry.azurecr.io/grafana-mcp-server:latest"
```

### 3. Build and Push Docker Image

```bash
# Build image
grafana-mcp-server-cli docker build --tag latest

# Login to Azure Container Registry
grafana-mcp-server-cli docker login --registry your-registry.azurecr.io

# Push image
grafana-mcp-server-cli docker push --tag latest --registry your-registry.azurecr.io
```

### 4. Deploy Infrastructure

```bash
grafana-mcp-server-cli iac deploy \
  --subscription-id <your-subscription-id> \
  --container-image your-registry.azurecr.io/grafana-mcp-server:latest \
  --state-rg rg-terraform-state \
  --state-storage stterraformstate
```

The CLI will:
1. Check Azure authentication
2. Create shared infrastructure (state storage)
3. Initialize Terraform with remote state
4. Validate configuration
5. Show deployment plan
6. Apply infrastructure changes

### 5. Verify Deployment

```bash
# Get deployment outputs
grafana-mcp-server-cli iac output

# Test the endpoint
curl https://<your-app-fqdn>/health
```

## Detailed Deployment

### Step 1: Shared Infrastructure

The CLI automatically creates shared infrastructure for Terraform state:

```bash
# This is done automatically by the CLI, but you can verify:
az group show --name rg-terraform-state
az storage account show --name stterraformstate --resource-group rg-terraform-state
```

Manual creation (if needed):

```bash
# Create resource group
az group create --name rg-terraform-state --location eastus

# Create storage account
az storage account create \
  --name stterraformstate \
  --resource-group rg-terraform-state \
  --location eastus \
  --sku Standard_LRS

# Create container
az storage container create \
  --name tfstate \
  --account-name stterraformstate
```

### Step 2: Container Image

Build and push your container image:

```bash
# Build with specific platform (for ARM-based builds)
grafana-mcp-server-cli docker build \
  --tag v1.0.0 \
  --platform linux/amd64

# Or build manually
docker build \
  --platform linux/amd64 \
  -t your-registry.azurecr.io/grafana-mcp-server:v1.0.0 \
  .

# Login to registry
grafana-mcp-server-cli docker login --registry your-registry.azurecr.io

# Push image
grafana-mcp-server-cli docker push --tag v1.0.0 --registry your-registry.azurecr.io
```

### Step 3: Terraform Configuration

Configure `terraform/terraform.tfvars`:

```hcl
# Required variables
subscription_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
resource_group_name = "rg-grafana-mcp-server-prod"
location = "eastus"
environment = "prod"

# Container configuration
mcp_container_image = "myregistry.azurecr.io/grafana-mcp-server:v1.0.0"
mcp_container_cpu = 0.5
mcp_container_memory = "1Gi"

# Scaling configuration
mcp_min_replicas = 2
mcp_max_replicas = 10

# Ingress configuration
mcp_ingress_external = true
mcp_ingress_target_port = 8000
mcp_ingress_allow_insecure = false

# Environment variables
mcp_env_vars = {
  MCP_NAME = "GrafanaMcpServer"
  LOG_LEVEL = "INFO"
  ENVIRONMENT = "production"
}

# Secrets (use Azure Key Vault in production)
# mcp_secrets = {
#   API_KEY = "sensitive-value"
# }
```

### Step 4: Deploy Infrastructure

Using CLI (recommended):

```bash
grafana-mcp-server-cli iac deploy \
  --subscription-id <sub-id> \
  --resource-group <rg-name> \
  --container-image <image> \
  --state-rg rg-terraform-state \
  --state-storage stterraformstate \
  --state-container tfstate
```

Using Terraform directly:

```bash
cd terraform

# Initialize with remote state
terraform init \
  -backend-config="resource_group_name=rg-terraform-state" \
  -backend-config="storage_account_name=stterraformstate" \
  -backend-config="container_name=tfstate" \
  -backend-config="key=grafana-mcp-server.tfstate"

# Review plan
terraform plan \
  -var="subscription_id=<sub-id>" \
  -var="mcp_container_image=<image>" \
  -out=tfplan

# Apply changes
terraform apply tfplan
```

### Step 5: Verify Deployment

```bash
# Get outputs
grafana-mcp-server-cli iac output

# Or with Terraform
cd terraform
terraform output

# Test endpoint
FQDN=$(terraform output -raw mcp_fqdn)
curl https://$FQDN/health

# View logs
az containerapp logs show \
  --name <app-name> \
  --resource-group <rg-name> \
  --follow
```

## Configuration

### Environment Variables

Configure in `terraform.tfvars`:

```hcl
mcp_env_vars = {
  # MCP configuration
  MCP_NAME = "GrafanaMcpServer"
  MCP_VERSION = "0.1.0"
  
  # Logging
  LOG_LEVEL = "INFO"
  LOG_FORMAT = "json"
  
  # Performance
  WORKERS = "2"
  
  # Environment
  ENVIRONMENT = "production"
}
```

### Secrets Management

**Option 1: Azure Key Vault (Recommended)**

```hcl
# Store secrets in Key Vault
az keyvault secret set \
  --vault-name my-keyvault \
  --name api-key \
  --value "secret-value"

# Reference in Container App (manual step after deployment)
az containerapp secret set \
  --name <app-name> \
  --resource-group <rg-name> \
  --secrets api-key=keyvaultref:<key-vault-url>/secrets/api-key
```

**Option 2: Terraform Secrets (Development Only)**

```hcl
mcp_secrets = {
  API_KEY = "dev-secret-value"
}
```

⚠️ **Warning**: Never commit secrets to git. Use terraform.tfvars (gitignored) or environment variables.

### Scaling Configuration

```hcl
# Auto-scaling
mcp_min_replicas = 1
mcp_max_replicas = 10

# Resource limits
mcp_container_cpu = 0.5
mcp_container_memory = "1Gi"
```

Scaling triggers:
- HTTP request rate
- CPU utilization (> 70%)
- Memory utilization (> 80%)

### Networking

**Internal Ingress (default)**:

```hcl
mcp_ingress_external = false
```

Accessible only within Virtual Network.

**External Ingress**:

```hcl
mcp_ingress_external = true
mcp_ingress_allow_insecure = false  # Force HTTPS
```

Accessible from internet with HTTPS.

## CLI Reference

### Infrastructure Commands

#### Deploy

```bash
grafana-mcp-server-cli iac deploy [OPTIONS]

Options:
  --subscription-id TEXT      Azure subscription ID [required]
  --resource-group TEXT       Resource group name (from tfvars)
  --container-image TEXT      Container image [required]
  --state-rg TEXT            State resource group [default: rg-terraform-state]
  --state-storage TEXT       State storage account [default: stterraformstate]
  --state-container TEXT     State container [default: tfstate]
  --auto-approve             Skip approval prompts
  --help                     Show help message
```

#### Destroy

```bash
grafana-mcp-server-cli iac destroy [OPTIONS]

Options:
  --subscription-id TEXT      Azure subscription ID [required]
  --state-rg TEXT            State resource group
  --state-storage TEXT       State storage account
  --state-container TEXT     State container
  --help                     Show help message
```

#### Output

```bash
grafana-mcp-server-cli iac output

Shows:
  - Resource Group ID
  - Container App Environment ID
  - MCP Server FQDN
  - MCP Server URL
  - Container Registry info
```

### Docker Commands

#### Build

```bash
grafana-mcp-server-cli docker build [OPTIONS]

Options:
  --tag TEXT                 Image tag [default: latest]
  --registry TEXT           Registry URL (optional)
  --platform TEXT           Platform [default: linux/amd64]
  --help                    Show help message
```

#### Push

```bash
grafana-mcp-server-cli docker push [OPTIONS]

Options:
  --tag TEXT                Image tag [required]
  --registry TEXT           Registry URL [required]
  --help                    Show help message
```

#### Login

```bash
grafana-mcp-server-cli docker login [OPTIONS]

Options:
  --registry TEXT           Registry URL [required]
  --help                    Show help message
```

## Troubleshooting

### Common Issues

#### 1. Azure Authentication Failed

```
Error: Failed to authenticate with Azure
```

**Solution**:
```bash
# Re-login
az login

# Verify authentication
az account show

# Check subscription
az account list --output table
```

#### 2. Container Image Not Found

```
Error: Failed to pull image
```

**Solution**:
```bash
# Verify image exists
az acr repository show \
  --name <registry-name> \
  --image grafana-mcp-server:latest

# Check image tag
az acr repository show-tags \
  --name <registry-name> \
  --repository grafana-mcp-server

# Re-push image
grafana-mcp-server-cli docker push --tag latest --registry <registry-url>
```

#### 3. Terraform State Locked

```
Error: state is locked
```

**Solution**:
```bash
# Force unlock (use with caution)
cd terraform
terraform force-unlock <lock-id>

# Or wait for lock to expire (usually 20 minutes)
```

#### 4. Container App Not Starting

**Check logs**:
```bash
az containerapp logs show \
  --name <app-name> \
  --resource-group <rg-name> \
  --follow
```

**Common causes**:
- Wrong container image
- Missing environment variables
- Port mismatch in ingress configuration
- Insufficient resources (CPU/memory)

#### 5. Permission Denied

```
Error: Insufficient permissions
```

**Solution**:
```bash
# Check your role
az role assignment list --assignee $(az account show --query user.name -o tsv)

# Required roles:
# - Contributor or Owner on subscription
# - Or specific roles: Container Apps Contributor, ACR Push/Pull
```

### Debug Commands

```bash
# View Container App details
az containerapp show \
  --name <app-name> \
  --resource-group <rg-name>

# Check revisions
az containerapp revision list \
  --name <app-name> \
  --resource-group <rg-name>

# View environment
az containerapp env show \
  --name <env-name> \
  --resource-group <rg-name>

# Check resource group
az group show --name <rg-name>
```

## Advanced Topics

### Multi-Environment Deployment

Use Terraform workspaces:

```bash
# Create dev environment
cd terraform
terraform workspace new dev
terraform plan -var-file="environments/dev.tfvars"
terraform apply -var-file="environments/dev.tfvars"

# Create prod environment
terraform workspace new prod
terraform plan -var-file="environments/prod.tfvars"
terraform apply -var-file="environments/prod.tfvars"

# List workspaces
terraform workspace list

# Switch workspace
terraform workspace select dev
```

### Blue-Green Deployment

```bash
# Deploy new version
grafana-mcp-server-cli docker build --tag v2.0.0
grafana-mcp-server-cli docker push --tag v2.0.0 --registry <registry>

# Update terraform.tfvars with new image
mcp_container_image = "registry.azurecr.io/grafana-mcp-server:v2.0.0"

# Deploy (creates new revision)
grafana-mcp-server-cli iac deploy --auto-approve

# Test new revision
az containerapp revision list --name <app-name> --resource-group <rg-name>

# Traffic splitting (if needed)
az containerapp ingress traffic set \
  --name <app-name> \
  --resource-group <rg-name> \
  --revision-weight <revision1>=50 <revision2>=50

# Rollback if issues
az containerapp ingress traffic set \
  --name <app-name> \
  --resource-group <rg-name> \
  --revision-weight <old-revision>=100
```

### Custom Domain

```bash
# Add custom domain
az containerapp hostname add \
  --hostname mcp.example.com \
  --name <app-name> \
  --resource-group <rg-name>

# Bind certificate
az containerapp hostname bind \
  --hostname mcp.example.com \
  --name <app-name> \
  --resource-group <rg-name> \
  --certificate <cert-name>
```

### Monitoring Setup

```bash
# Create Application Insights
az monitor app-insights component create \
  --app <app-name>-insights \
  --location <location> \
  --resource-group <rg-name>

# Link to Container App
INSTRUMENTATION_KEY=$(az monitor app-insights component show \
  --app <app-name>-insights \
  --resource-group <rg-name> \
  --query instrumentationKey -o tsv)

# Update environment variables in terraform.tfvars
mcp_env_vars = {
  APPLICATIONINSIGHTS_CONNECTION_STRING = "InstrumentationKey=$INSTRUMENTATION_KEY"
}
```

### Private Networking

Update terraform.tfvars:

```hcl
# Create internal ingress only
mcp_ingress_external = false

# Access via VNet peering or VPN
# Container App gets private IP within the environment's subnet
```

### Cost Optimization

```bash
# Use consumption-based pricing
mcp_min_replicas = 0  # Scale to zero when idle
mcp_max_replicas = 5

# Use smaller resources for dev
mcp_container_cpu = 0.25
mcp_container_memory = "0.5Gi"

# Check costs
az consumption usage list \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --query "[?contains(instanceName, 'grafana-mcp-server')]"
```

## CI/CD Integration

See [CI/CD Guide](CI_CD.md) for detailed pipeline configurations.

## Security Best Practices

See [Security Guide](SECURITY.md) for comprehensive security recommendations.

## Next Steps

- [QUICK_START.md](QUICK_START.md) - Development setup
- [ARCHITECTURE.md](ARCHITECTURE.md) - System architecture
- [API.md](API.md) - API documentation
- [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines
