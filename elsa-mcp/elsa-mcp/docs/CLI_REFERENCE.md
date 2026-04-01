# MCP Template CLI Reference

**Version:** 0.1.0  
**Last Updated:** December 17, 2025

---

## Overview

The MCP Template CLI is a specialized command-line tool for **deploying and managing MCP servers on Azure**. It automates infrastructure provisioning using Terraform and handles Docker image lifecycle management.

**Key Features:**
- 🏗️ Automated Azure infrastructure deployment
- 🐳 Docker build, push, and registry authentication
- 📦 Terraform state management
- ✅ Azure prerequisite validation
- 🔒 Secure deployment workflows
- 🚀 CI/CD ready

---

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Command Reference](#command-reference)
   - [IAC Commands](#iac-commands)
   - [Docker Commands](#docker-commands)
4. [Deployment Workflows](#deployment-workflows)
5. [Configuration](#configuration)
6. [CI/CD Integration](#cicd-integration)
7. [Troubleshooting](#troubleshooting)

---

## Installation

### Prerequisites

- Python 3.11+
- Azure CLI (`az`)
- Docker
- Terraform 1.0+
- Azure subscription with appropriate permissions

### Install CLI

The CLI is included when you scaffold an MCP server from the template:

```bash
# Scaffold new MCP server
agenticai scaffold my-mcp-server --template mcp-server

# Navigate to project
cd my-mcp-server

# Install with CLI dependencies
pip install -e ".[cli]"
```

### Verify Installation

```bash
# Check CLI is available
my-mcp-server-cli --help

# Check version
my-mcp-server-cli version
```

---

## Quick Start

### 5-Minute Deployment

```bash
# 1. Login to Azure
az login

# 2. Build Docker image
my-mcp-server-cli docker build \
  --tag v1.0 \
  --registry myacr.azurecr.io

# 3. Login to container registry
my-mcp-server-cli docker login --registry myacr.azurecr.io

# 4. Push image
my-mcp-server-cli docker push \
  --tag v1.0 \
  --registry myacr.azurecr.io

# 5. Deploy infrastructure
my-mcp-server-cli iac deploy \
  --subscription-id "your-subscription-id" \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001" \
  --container-image "myacr.azurecr.io/my-mcp-server:v1.0" \
  --auto-approve

# 6. Get deployment info
my-mcp-server-cli iac output
```

---

## Command Reference

### Global Commands

#### `version`

Show CLI version information.

```bash
my-mcp-server-cli version
```

**Output:**
```
my-mcp-server-cli version 0.1.0
```

---

### IAC Commands

Infrastructure as Code (Terraform) management commands.

#### `iac deploy`

Deploy MCP server infrastructure to Azure with full automation.

**Usage:**
```bash
my-mcp-server-cli iac deploy [OPTIONS]
```

**Required Options:**

| Option | Short | Description | Example |
|--------|-------|-------------|---------|
| `--subscription-id` | `-s` | Azure subscription ID | `abc123...` |
| `--state-rg` | | Terraform state resource group | `rg-terraform-state` |
| `--state-storage` | | Terraform state storage account | `sttfstate001` |
| `--container-image` | `-i` | Container image for MCP server | `myacr.azurecr.io/mcp:v1` |

**Optional Options:**

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--resource-group` | `-g` | `rg-{name}-dev` | Target resource group name |
| `--location` | `-l` | `eastus` | Azure region |
| `--auto-approve` | `-y` | `false` | Skip confirmation prompt |
| `--ensure-shared-infra` | | `true` | Create state storage if missing |
| `--skip-shared-infra` | | `false` | Skip shared infra creation |
| `--dry-run` | | `false` | Show commands without executing |

**What It Does:**

1. ✅ **Validates Azure CLI login** - Ensures `az login` is active
2. ✅ **Creates shared infrastructure** - State storage account, resource group, blob container
3. ✅ **Initializes Terraform** - Sets up remote backend with state configuration
4. ✅ **Validates configuration** - Runs `terraform validate`
5. ✅ **Generates plan** - Creates execution plan (`tfplan`)
6. ✅ **Applies changes** - Deploys infrastructure (with confirmation unless `--auto-approve`)
7. ✅ **Shows outputs** - Displays deployment information

**Infrastructure Components Deployed:**

- 📦 **Resource Group** - Container for all resources
- 🏢 **Log Analytics Workspace** - Centralized logging and monitoring
- 🌐 **Container App Environment** - Managed Kubernetes environment
- 🚀 **MCP Server Container App** - Your MCP server running in a container
- 📊 **Container Registry** (optional) - Private Docker registry

**Examples:**

```bash
# Basic deployment
my-mcp-server-cli iac deploy \
  --subscription-id "abc123-def456-..." \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001" \
  --container-image "myacr.azurecr.io/my-mcp:v1.0"

# Auto-approve (CI/CD)
my-mcp-server-cli iac deploy \
  --subscription-id "abc123..." \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001" \
  --container-image "myacr.azurecr.io/my-mcp:v1.0" \
  --auto-approve

# Custom location and resource group
my-mcp-server-cli iac deploy \
  --subscription-id "abc123..." \
  --resource-group "rg-my-mcp-prod" \
  --location "westus2" \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001" \
  --container-image "myacr.azurecr.io/my-mcp:v1.0"

# Dry run (preview only)
my-mcp-server-cli iac deploy \
  --subscription-id "abc123..." \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001" \
  --container-image "myacr.azurecr.io/my-mcp:v1.0" \
  --dry-run

# Skip shared infra creation (already exists)
my-mcp-server-cli iac deploy \
  --subscription-id "abc123..." \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001" \
  --container-image "myacr.azurecr.io/my-mcp:v1.0" \
  --skip-shared-infra
```

**Output Example:**

```
🤖 MCP Server Infrastructure Deployment
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

→ Checking Azure CLI login...
✓ Azure CLI authenticated

→ Ensuring shared infrastructure...
✓ State resource group: rg-terraform-state
✓ State storage account: sttfstate001
✓ Tfstate container ready

→ Initializing Terraform...
✓ Terraform initialized

→ Validating Terraform configuration...
✓ Configuration valid

→ Planning infrastructure changes...
✓ Plan created

→ Applying infrastructure changes...
✓ Deployment completed successfully!

→ Infrastructure outputs:
mcp_server_fqdn = "my-mcp-server.eastus.azurecontainerapps.io"
mcp_server_url = "https://my-mcp-server.eastus.azurecontainerapps.io"
resource_group_id = "/subscriptions/.../resourceGroups/rg-my-mcp-dev"
```

---

#### `iac destroy`

Destroy deployed infrastructure and clean up all Azure resources.

**Usage:**
```bash
my-mcp-server-cli iac destroy [OPTIONS]
```

**Required Options:**

| Option | Short | Description |
|--------|-------|-------------|
| `--subscription-id` | `-s` | Azure subscription ID |
| `--state-rg` | | Terraform state resource group |
| `--state-storage` | | Terraform state storage account |

**Optional Options:**

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--auto-approve` | `-y` | `false` | Skip confirmation prompt |

**What It Does:**

1. ⚠️ **Shows warning** - Displays destruction warning
2. ❓ **Confirms action** - Prompts for confirmation (unless `--auto-approve`)
3. 🔧 **Initializes Terraform** - Connects to remote state
4. 💥 **Destroys infrastructure** - Removes all deployed resources
5. ✅ **Confirms deletion** - Shows success/failure status

**Examples:**

```bash
# Destroy with confirmation
my-mcp-server-cli iac destroy \
  --subscription-id "abc123..." \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001"

# Auto-approve destruction (dangerous!)
my-mcp-server-cli iac destroy \
  --subscription-id "abc123..." \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001" \
  --auto-approve
```

**Output Example:**

```
⚠  WARNING: This will destroy all infrastructure!
Are you sure you want to continue? [y/N]: y

→ Initializing Terraform...
✓ Terraform initialized

→ Destroying infrastructure...
✓ Infrastructure destroyed
```

⚠️ **Warning:** This action is irreversible and will delete all deployed resources.

---

#### `iac output`

Show Terraform outputs from the deployed infrastructure.

**Usage:**
```bash
my-mcp-server-cli iac output
```

**No options required** - reads from current Terraform state.

**What It Shows:**

- 🌐 **MCP Server FQDN** - Fully qualified domain name
- 🔗 **MCP Server URL** - HTTPS endpoint
- 📦 **Resource Group ID** - Azure resource group identifier
- 🌍 **Container App Environment ID** - Managed environment ID
- 📊 **Container Registry URL** - Private registry login server
- 🔑 **Admin Credentials** - Registry admin username/password (if enabled)

**Example:**

```bash
my-mcp-server-cli iac output
```

**Output:**

```
container_app_environment_id = "/subscriptions/.../providers/Microsoft.App/managedEnvironments/cae-my-mcp-dev"
container_registry_login_server = "myacr.azurecr.io"
mcp_server_fqdn = "my-mcp-server.eastus.azurecontainerapps.io"
mcp_server_id = "/subscriptions/.../providers/Microsoft.App/containerApps/ca-my-mcp-server-dev"
mcp_server_url = "https://my-mcp-server.eastus.azurecontainerapps.io"
resource_group_id = "/subscriptions/.../resourceGroups/rg-my-mcp-dev"
```

---

### Docker Commands

Container image management commands.

#### `docker build`

Build Docker image for the MCP server.

**Usage:**
```bash
my-mcp-server-cli docker build [OPTIONS]
```

**Options:**

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--tag` | `-t` | `latest` | Image tag |
| `--registry` | `-r` | None | Container registry URL |
| `--platform` | | `linux/amd64` | Target platform |

**What It Does:**

1. 📦 **Builds Docker image** - Uses Dockerfile in project root
2. 🏷️ **Tags image** - Applies specified tag
3. 🌐 **Adds registry prefix** - If registry URL provided
4. 🖥️ **Platform targeting** - Builds for specified platform

**Image Naming:**

- **Without registry:** `{mcp-name}:{tag}`
- **With registry:** `{registry}/{mcp-name}:{tag}`

**Examples:**

```bash
# Build locally with default tag
my-mcp-server-cli docker build

# Build with custom tag
my-mcp-server-cli docker build --tag v1.0.0

# Build for registry
my-mcp-server-cli docker build \
  --tag v1.0.0 \
  --registry myacr.azurecr.io

# Build for ARM64 (Apple Silicon)
my-mcp-server-cli docker build \
  --tag v1.0.0 \
  --platform linux/arm64

# Build for multi-platform
my-mcp-server-cli docker build \
  --tag v1.0.0 \
  --registry myacr.azurecr.io \
  --platform linux/amd64,linux/arm64
```

**Output Example:**

```
→ Building Docker image: myacr.azurecr.io/my-mcp-server:v1.0.0
[+] Building 45.3s (12/12) FINISHED
 => [internal] load build definition from Dockerfile
 => => transferring dockerfile: 1.2kB
 => [internal] load .dockerignore
 => => transferring context: 52B
 => [internal] load metadata for docker.io/library/python:3.11-slim
 => [1/7] FROM docker.io/library/python:3.11-slim
 => [2/7] WORKDIR /app
 => [3/7] COPY pyproject.toml .
 => [4/7] RUN pip install --no-cache-dir -e .
 => [5/7] COPY src ./src
 => [6/7] COPY cli ./cli
 => [7/7] COPY .env.example .env
 => exporting to image
 => => exporting layers
 => => writing image sha256:abc123...
 => => naming to myacr.azurecr.io/my-mcp-server:v1.0.0
✓ Image built: myacr.azurecr.io/my-mcp-server:v1.0.0
```

---

#### `docker push`

Push Docker image to container registry.

**Usage:**
```bash
my-mcp-server-cli docker push [OPTIONS]
```

**Required Options:**

| Option | Short | Description |
|--------|-------|-------------|
| `--registry` | `-r` | Container registry URL |

**Optional Options:**

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--tag` | `-t` | `latest` | Image tag to push |

**Prerequisites:**

- Docker image must be built with registry prefix
- Must be logged in to registry (`docker login` or `docker login`)

**Examples:**

```bash
# Push latest tag
my-mcp-server-cli docker push --registry myacr.azurecr.io

# Push specific version
my-mcp-server-cli docker push \
  --tag v1.0.0 \
  --registry myacr.azurecr.io
```

**Output Example:**

```
→ Pushing image: myacr.azurecr.io/my-mcp-server:v1.0.0
The push refers to repository [myacr.azurecr.io/my-mcp-server]
abc123: Pushed
def456: Pushed
ghi789: Pushed
v1.0.0: digest: sha256:xyz789... size: 1234
✓ Image pushed: myacr.azurecr.io/my-mcp-server:v1.0.0
```

---

#### `docker login`

Authenticate with Azure Container Registry.

**Usage:**
```bash
my-mcp-server-cli docker login [OPTIONS]
```

**Required Options:**

| Option | Short | Description |
|--------|-------|-------------|
| `--registry` | `-r` | Container registry URL |

**What It Does:**

1. 🔑 **Extracts registry name** - Removes `.azurecr.io` and `https://` prefix
2. 🔐 **Uses Azure CLI** - Runs `az acr login --name {registry-name}`
3. ✅ **Stores credentials** - Saves Docker credentials locally

**Prerequisites:**

- Azure CLI must be installed (`az`)
- Must be logged in to Azure (`az login`)
- Must have ACR pull/push permissions

**Examples:**

```bash
# Login to ACR
my-mcp-server-cli docker login --registry myacr.azurecr.io

# Login to ACR (with https prefix)
my-mcp-server-cli docker login --registry https://myacr.azurecr.io
```

**Output Example:**

```
→ Logging in to: myacr.azurecr.io
Login Succeeded
✓ Logged in to myacr.azurecr.io
```

---

## Deployment Workflows

### Development Deployment

For testing in a development environment:

```bash
# 1. Build and test locally
docker build -t my-mcp-server:dev .
docker run -p 8000:8000 my-mcp-server:dev

# 2. Build for Azure
my-mcp-server-cli docker build --tag dev

# 3. Deploy to dev environment
my-mcp-server-cli iac deploy \
  --subscription-id "$AZURE_SUBSCRIPTION_ID" \
  --resource-group "rg-my-mcp-dev" \
  --location "eastus" \
  --state-rg "rg-terraform-state-dev" \
  --state-storage "sttfstatedev001" \
  --container-image "my-mcp-server:dev" \
  --auto-approve
```

---

### Production Deployment

For production with proper versioning and registry:

```bash
# 1. Tag with version
VERSION="1.0.0"

# 2. Build production image
my-mcp-server-cli docker build \
  --tag v${VERSION} \
  --registry myacr.azurecr.io \
  --platform linux/amd64

# 3. Login to registry
my-mcp-server-cli docker login --registry myacr.azurecr.io

# 4. Push to registry
my-mcp-server-cli docker push \
  --tag v${VERSION} \
  --registry myacr.azurecr.io

# 5. Deploy to production
my-mcp-server-cli iac deploy \
  --subscription-id "$AZURE_SUBSCRIPTION_ID" \
  --resource-group "rg-my-mcp-prod" \
  --location "eastus" \
  --state-rg "rg-terraform-state-prod" \
  --state-storage "sttfstateprod001" \
  --container-image "myacr.azurecr.io/my-mcp-server:v${VERSION}" \
  --auto-approve

# 6. Verify deployment
my-mcp-server-cli iac output
```

---

### Blue-Green Deployment

Deploy to a new environment and switch traffic:

```bash
# Deploy to green environment
my-mcp-server-cli iac deploy \
  --subscription-id "$AZURE_SUBSCRIPTION_ID" \
  --resource-group "rg-my-mcp-green" \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001" \
  --container-image "myacr.azurecr.io/my-mcp:v2.0.0"

# Test green environment
curl https://my-mcp-green.eastus.azurecontainerapps.io/health

# Switch traffic (update DNS/load balancer)
# ...

# Destroy blue environment
my-mcp-server-cli iac destroy \
  --subscription-id "$AZURE_SUBSCRIPTION_ID" \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001"
```

---

### Rollback Procedure

Rollback to previous version:

```bash
# 1. Get previous version from registry
az acr repository show-tags \
  --name myacr \
  --repository my-mcp-server \
  --orderby time_desc

# 2. Redeploy with previous version
my-mcp-server-cli iac deploy \
  --subscription-id "$AZURE_SUBSCRIPTION_ID" \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001" \
  --container-image "myacr.azurecr.io/my-mcp-server:v1.0.0" \
  --auto-approve
```

---

## Configuration

### Environment Variables

The CLI respects these environment variables:

```bash
# Azure subscription
export AZURE_SUBSCRIPTION_ID="abc123-def456-..."

# Terraform state
export TF_STATE_RG="rg-terraform-state"
export TF_STATE_STORAGE="sttfstate001"

# Container registry
export CONTAINER_REGISTRY="myacr.azurecr.io"

# Version
export MCP_VERSION="1.0.0"
```

**Use in commands:**

```bash
my-mcp-server-cli iac deploy \
  --subscription-id "$AZURE_SUBSCRIPTION_ID" \
  --state-rg "$TF_STATE_RG" \
  --state-storage "$TF_STATE_STORAGE" \
  --container-image "$CONTAINER_REGISTRY/my-mcp-server:v$MCP_VERSION"
```

---

### Terraform Variables

Configure deployment by creating `terraform/terraform.tfvars`:

```hcl
# Required
subscription_id              = "abc123-def456-..."
resource_group_name          = "rg-my-mcp-prod"
location                     = "eastus"
state_resource_group_name    = "rg-terraform-state"
state_storage_account_name   = "sttfstate001"
mcp_container_image          = "myacr.azurecr.io/my-mcp-server:v1.0.0"

# Optional - Container configuration
mcp_container_cpu            = 0.5        # CPU cores
mcp_container_memory         = "1Gi"      # Memory
mcp_min_replicas             = 1          # Min instances
mcp_max_replicas             = 3          # Max instances

# Optional - Ingress
mcp_ingress_external         = true       # External access
mcp_ingress_target_port      = 8000       # Container port
mcp_ingress_allow_insecure   = false      # HTTPS only

# Optional - Environment variables
mcp_env_vars = {
  "LOG_LEVEL"          = "INFO"
  "MCP_AUTH_ENABLED"   = "true"
  "AZURE_TENANT_ID"    = "fcb2b37b-..."
}

# Optional - Secrets (from Key Vault)
mcp_secrets = {
  "DATABASE_PASSWORD" = "secret-from-keyvault"
  "API_KEY"           = "another-secret"
}
```

---

### Azure Prerequisites

**Required Azure Resources:**

1. **Azure Subscription** - Active subscription with Owner/Contributor role
2. **Resource Group (State)** - For Terraform state storage
3. **Storage Account (State)** - For `.tfstate` files
4. **Container Registry** - For Docker images (or use Docker Hub)

**Create prerequisites manually:**

```bash
# Login
az login

# Create state resource group
az group create \
  --name rg-terraform-state \
  --location eastus

# Create state storage account
az storage account create \
  --name sttfstate001 \
  --resource-group rg-terraform-state \
  --location eastus \
  --sku Standard_LRS \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false

# Create tfstate container
az storage container create \
  --name tfstate \
  --account-name sttfstate001 \
  --auth-mode login

# Create container registry
az acr create \
  --name myacr \
  --resource-group rg-terraform-state \
  --location eastus \
  --sku Basic \
  --admin-enabled true
```

**Or let CLI create them:**

```bash
# CLI creates state resources automatically
my-mcp-server-cli iac deploy \
  --subscription-id "$AZURE_SUBSCRIPTION_ID" \
  --state-rg "rg-terraform-state" \
  --state-storage "sttfstate001" \
  --container-image "myacr.azurecr.io/my-mcp:v1.0" \
  --ensure-shared-infra  # This flag creates missing resources
```

---

## CI/CD Integration

### GitHub Actions

**`.github/workflows/deploy.yml`:**

```yaml
name: Deploy MCP Server

on:
  push:
    branches: [main]
    tags: ['v*']

env:
  AZURE_SUBSCRIPTION_ID: ${{ secrets.AZURE_SUBSCRIPTION_ID }}
  REGISTRY: myacr.azurecr.io
  IMAGE_NAME: my-mcp-server

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install CLI
        run: |
          pip install -e ".[cli]"
      
      - name: Azure login
        uses: azure/login@v2
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}
      
      - name: Set version
        id: version
        run: |
          if [[ "${{ github.ref }}" == refs/tags/* ]]; then
            VERSION=${GITHUB_REF#refs/tags/}
          else
            VERSION="dev-${GITHUB_SHA::7}"
          fi
          echo "version=${VERSION}" >> $GITHUB_OUTPUT
      
      - name: Build image
        run: |
          my-mcp-server-cli docker build \
            --tag ${{ steps.version.outputs.version }} \
            --registry ${{ env.REGISTRY }} \
            --platform linux/amd64
      
      - name: Login to ACR
        run: |
          my-mcp-server-cli docker login \
            --registry ${{ env.REGISTRY }}
      
      - name: Push image
        run: |
          my-mcp-server-cli docker push \
            --tag ${{ steps.version.outputs.version }} \
            --registry ${{ env.REGISTRY }}
      
      - name: Deploy to Azure
        run: |
          my-mcp-server-cli iac deploy \
            --subscription-id ${{ env.AZURE_SUBSCRIPTION_ID }} \
            --resource-group rg-my-mcp-prod \
            --location eastus \
            --state-rg rg-terraform-state \
            --state-storage sttfstate001 \
            --container-image ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${{ steps.version.outputs.version }} \
            --auto-approve
      
      - name: Show outputs
        run: |
          my-mcp-server-cli iac output
```

---

### Azure DevOps

**`azure-pipelines.yml`:**

```yaml
trigger:
  branches:
    include:
    - main
  tags:
    include:
    - v*

pool:
  vmImage: 'ubuntu-latest'

variables:
  azureSubscription: 'MyAzureConnection'
  registry: 'myacr.azurecr.io'
  imageName: 'my-mcp-server'
  
stages:
- stage: Build
  jobs:
  - job: BuildImage
    steps:
    - task: UsePythonVersion@0
      inputs:
        versionSpec: '3.11'
    
    - script: |
        pip install -e ".[cli]"
      displayName: 'Install CLI'
    
    - task: AzureCLI@2
      displayName: 'Build Docker Image'
      inputs:
        azureSubscription: $(azureSubscription)
        scriptType: 'bash'
        scriptLocation: 'inlineScript'
        inlineScript: |
          VERSION="${BUILD_SOURCEBRANCHNAME}"
          if [[ "$VERSION" == "main" ]]; then
            VERSION="dev-${BUILD_SOURCEVERSION:0:7}"
          fi
          
          my-mcp-server-cli docker build \
            --tag $VERSION \
            --registry $(registry) \
            --platform linux/amd64
          
          my-mcp-server-cli docker login --registry $(registry)
          my-mcp-server-cli docker push --tag $VERSION --registry $(registry)

- stage: Deploy
  dependsOn: Build
  jobs:
  - deployment: DeployToAzure
    environment: 'production'
    strategy:
      runOnce:
        deploy:
          steps:
          - task: AzureCLI@2
            displayName: 'Deploy Infrastructure'
            inputs:
              azureSubscription: $(azureSubscription)
              scriptType: 'bash'
              scriptLocation: 'inlineScript'
              inlineScript: |
                VERSION="${BUILD_SOURCEBRANCHNAME}"
                
                my-mcp-server-cli iac deploy \
                  --subscription-id $(AZURE_SUBSCRIPTION_ID) \
                  --resource-group rg-my-mcp-prod \
                  --state-rg rg-terraform-state \
                  --state-storage sttfstate001 \
                  --container-image $(registry)/$(imageName):$VERSION \
                  --auto-approve
                
                my-mcp-server-cli iac output
```

---

### GitLab CI

**`.gitlab-ci.yml`:**

```yaml
stages:
  - build
  - deploy

variables:
  AZURE_SUBSCRIPTION_ID: $AZURE_SUBSCRIPTION_ID
  REGISTRY: myacr.azurecr.io
  IMAGE_NAME: my-mcp-server

build:
  stage: build
  image: python:3.11
  before_script:
    - pip install -e ".[cli]"
    - az login --service-principal -u $AZURE_CLIENT_ID -p $AZURE_CLIENT_SECRET --tenant $AZURE_TENANT_ID
  script:
    - VERSION="${CI_COMMIT_TAG:-dev-${CI_COMMIT_SHORT_SHA}}"
    - my-mcp-server-cli docker build --tag $VERSION --registry $REGISTRY
    - my-mcp-server-cli docker login --registry $REGISTRY
    - my-mcp-server-cli docker push --tag $VERSION --registry $REGISTRY

deploy:
  stage: deploy
  image: python:3.11
  dependencies:
    - build
  before_script:
    - pip install -e ".[cli]"
    - az login --service-principal -u $AZURE_CLIENT_ID -p $AZURE_CLIENT_SECRET --tenant $AZURE_TENANT_ID
  script:
    - VERSION="${CI_COMMIT_TAG:-dev-${CI_COMMIT_SHORT_SHA}}"
    - |
      my-mcp-server-cli iac deploy \
        --subscription-id $AZURE_SUBSCRIPTION_ID \
        --state-rg rg-terraform-state \
        --state-storage sttfstate001 \
        --container-image $REGISTRY/$IMAGE_NAME:$VERSION \
        --auto-approve
    - my-mcp-server-cli iac output
  only:
    - main
    - tags
```

---

## Troubleshooting

### Common Issues

#### ❌ `Not logged in to Azure CLI`

**Error:**
```
❌ Not logged in to Azure CLI. Please run: az login
```

**Solution:**
```bash
az login
```

---

#### ❌ `Terraform init failed`

**Error:**
```
✗ Terraform init failed:
Error: Failed to get existing workspaces: containers.Client#ListBlobs: Failure responding to request
```

**Possible causes:**
1. Storage account doesn't exist
2. No permissions on storage account
3. Wrong storage account name

**Solutions:**

```bash
# Check storage account exists
az storage account show \
  --name sttfstate001 \
  --resource-group rg-terraform-state

# Grant permissions
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee "your-email@example.com" \
  --scope "/subscriptions/.../resourceGroups/rg-terraform-state/providers/Microsoft.Storage/storageAccounts/sttfstate001"

# Or let CLI create it
my-mcp-server-cli iac deploy \
  --ensure-shared-infra \
  ...
```

---

#### ❌ `Docker build failed`

**Error:**
```
✗ Build failed
```

**Solutions:**

```bash
# Check Docker is running
docker ps

# Check Dockerfile exists
ls -la Dockerfile

# Build manually to see detailed errors
docker build -t test .

# Check platform matches your system
my-mcp-server-cli docker build --platform linux/amd64
```

---

#### ❌ `Docker push failed: unauthorized`

**Error:**
```
✗ Push failed
unauthorized: authentication required
```

**Solution:**
```bash
# Login to registry first
my-mcp-server-cli docker login --registry myacr.azurecr.io

# Or use Azure CLI
az acr login --name myacr
```

---

#### ❌ `Container app deployment failed`

**Error:**
```
Error: creating Linux Container App: containerApps.ContainerAppsClient#CreateOrUpdate: Failure sending request
```

**Possible causes:**
1. Image doesn't exist in registry
2. Container App Environment not ready
3. Insufficient quota

**Solutions:**

```bash
# Verify image exists
az acr repository show \
  --name myacr \
  --repository my-mcp-server \
  --tag v1.0.0

# Check Container App Environment
az containerapp env list --resource-group rg-my-mcp-dev

# Check quota
az quota show \
  --resource-type containerApps \
  --scope /subscriptions/$AZURE_SUBSCRIPTION_ID
```

---

#### ❌ `Terraform state locked`

**Error:**
```
Error: Error acquiring the state lock
```

**Cause:** Previous terraform command didn't complete cleanly

**Solution:**

```bash
# Wait for lock to expire (usually 2 minutes)

# Or force unlock (dangerous!)
cd terraform
terraform force-unlock <lock-id>
```

---

### Debug Mode

Enable verbose logging:

```bash
# Terraform debug
export TF_LOG=DEBUG
my-mcp-server-cli iac deploy ...

# Azure CLI debug
export AZURE_CLI_DEBUG=1
my-mcp-server-cli docker login ...

# Docker debug
export DOCKER_BUILDKIT_STEP_LOG_MAX_SIZE=-1
my-mcp-server-cli docker build ...
```

---

### Getting Help

**Check CLI version:**
```bash
my-mcp-server-cli version
```

**Get command help:**
```bash
my-mcp-server-cli --help
my-mcp-server-cli iac --help
my-mcp-server-cli iac deploy --help
```

**Dry run deployment:**
```bash
my-mcp-server-cli iac deploy --dry-run ...
```

**Check Terraform plan:**
```bash
cd terraform
terraform init
terraform plan
```

---

## Best Practices

### 1. **Use Semantic Versioning**

Tag images with semantic versions:

```bash
my-mcp-server-cli docker build --tag v1.2.3
```

### 2. **Separate Environments**

Use different state storage for each environment:

```bash
# Dev
--state-rg "rg-terraform-state-dev"
--state-storage "sttfstatedev001"

# Prod
--state-rg "rg-terraform-state-prod"
--state-storage "sttfstateprod001"
```

### 3. **Always Dry Run First**

Preview changes before applying:

```bash
my-mcp-server-cli iac deploy --dry-run ...
```

### 4. **Use Auto-Approve in CI/CD Only**

Manual deployments should require confirmation:

```bash
# Manual: require confirmation
my-mcp-server-cli iac deploy ...

# CI/CD: auto-approve
my-mcp-server-cli iac deploy --auto-approve ...
```

### 5. **Store Secrets in Key Vault**

Never hardcode secrets in Terraform variables:

```hcl
mcp_secrets = {
  "API_KEY" = data.azurerm_key_vault_secret.api_key.value
}
```

### 6. **Tag Images with Git SHA**

Track deployments to commits:

```bash
VERSION="v1.0.0-${GIT_SHA:0:7}"
my-mcp-server-cli docker build --tag $VERSION
```

### 7. **Monitor Deployments**

Check deployment status:

```bash
# After deployment
my-mcp-server-cli iac output

# Check container app
az containerapp show \
  --name ca-my-mcp-server-dev \
  --resource-group rg-my-mcp-dev

# Check logs
az containerapp logs show \
  --name ca-my-mcp-server-dev \
  --resource-group rg-my-mcp-dev \
  --follow
```

---

## Comparison with AgenticAI CLI

| Feature | AgenticAI CLI | MCP Template CLI |
|---------|---------------|------------------|
| **Purpose** | Development & Testing | Deployment & Operations |
| **Scope** | All agent types | MCP servers only |
| **Commands** | `list`, `run`, `dev`, `test`, `scaffold` | `iac`, `docker` |
| **Focus** | Local development | Azure production |
| **Usage Phase** | Development → Testing | Build → Deploy |
| **Entry Point** | `agenticai` / `aa` | `{mcp-name}-cli` |
| **Terraform** | ❌ Not included | ✅ Full support |
| **Docker** | ❌ Basic (build only) | ✅ Build, push, login |
| **Azure Integration** | ❌ None | ✅ Complete |

**Complementary Tools:**
- Use **AgenticAI CLI** during development
- Use **MCP Template CLI** for deployment

---

## Additional Resources

- **AgenticAI SDK Documentation:** [../agentic_ai_sdk/docs/README.md](../../agentic_ai_sdk/docs/README.md)
- **Terraform Azure Provider:** https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs
- **Azure Container Apps:** https://learn.microsoft.com/en-us/azure/container-apps/
- **FastMCP Documentation:** https://github.com/jlowin/fastmcp
- **Model Context Protocol:** https://modelcontextprotocol.io/

---

## Support

For issues or questions:
1. Check [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment guide
2. Review [Troubleshooting](#troubleshooting) section above
3. Check Terraform logs in `terraform/` directory
4. Open issue on GitHub repository

---

**Document Version:** 1.0.0  
**Last Updated:** December 17, 2025  
**Maintainer:** Bayer Agentic Foundation Team
