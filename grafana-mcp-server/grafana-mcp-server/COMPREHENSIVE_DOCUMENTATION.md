# Agentic AI MCP Server - Comprehensive Documentation

## Overview

This MCP (Model Context Protocol) server is a template for building, running, and deploying MCP-compliant services. It is built on the FastMCP framework and is designed for rapid integration with LLMs, agentic platforms, and enterprise workflows. The template includes example tools, authentication, telemetry, and full Docker/Terraform/CI support.

---

## What This MCP Server Does

- **Implements MCP protocol** for agent/LLM integration
- **Provides example tools** (hello world, echo, math, string ops)
- **Supports authentication** (Azure AD/JWT, optional)
- **Telemetry ready** (OpenTelemetry, optional)
- **Production-ready Dockerfile** and Terraform for Azure
- **Easy local development and CI/CD**

---

## Technology Stack

- **Framework:** FastMCP (Python)
- **Containerization:** Docker, Docker Compose
- **Infrastructure:** Terraform, Azure
- **CLI:** agenticai CLI (recommended for all operations)
- **CI/CD:** GitHub Actions, Azure DevOps

---

## Prerequisites

### System Requirements
- **Python:** 3.11 or higher
- **Docker:** 20.10 or higher
- **Terraform:** 1.0 or higher (for Azure deployment)
- **Azure CLI:** (for Azure deployment)
- **Git:** (for source control)

### Familiarity Required
- **Python packaging and virtual environments**
- **Docker basics**
- **Azure basics (resource groups, container registry, etc.)**
- **Terraform basics**

---


## Installing the agenticai CLI

You need the `agenticai` CLI to build, run, and manage the MCP server via Docker and Terraform.

**Development/Editable Install (Recommended):**

```powershell
uv pip install -e agentic_ai_cli
# or, from inside the CLI directory:
uv pip install -e .
```

**From Artifactory (for Bayer employees):**

```powershell
uv pip install agentic-ai-cli --index-url https://artifactory.bayer.com/artifactory/api/pypi/agf-pypi-dev-cli/simple
```

Verify with:

```powershell
agenticai --help
```

---

## Getting the MCP Server To Run From

If you are using the template, scaffold a new MCP server project with the agenticai CLI (recommended example):

```bash
agenticai scaffold my-mcp-docs-server --template mcp_server --no-interactive --description "Documentation/validation MCP server" --author-name "Your Name" --author-email "your.email@example.com" --port 3000
cd my-mcp-docs-server
uv pip install -e .
my-mcp-docs-server
```

## Environment Variables

Copy `.env.example` to `.env` and edit as needed:

```bash
cp .env.example .env
```

Key variables:
- `MCP_SERVER_PORT` (default: 8000)
- `LOG_LEVEL` (default: INFO)
- `MCP_AUTH_ENABLED` (true/false)
- `AZURE_TENANT_ID`, `MANAGED_IDENTITY_CLIENT_ID` (for Azure auth)

---

## Building and Running the MCP Server with agenticai CLI

The `agenticai` CLI provides a streamlined way to build, run, and manage the MCP server as a Docker container.

### 1. Build the MCP Docker Image

```bash
agenticai mcp build <mcp-server-dir-name> --tag local
# Options:
#   --tag <name>         # Tag for the Docker image (default: local)
#   --proxy <url>        # (Optional) HTTP proxy for build
#   --workspace <path>   # Path to workspace (default: current dir)
```

> **Proxy Note:**
> If you are behind a corporate proxy, use the `--proxy` option:
> ```bash
> agenticai mcp build <mcp-server-dir-name> --proxy http://your.proxy.address:port
> ```

### 2. Run the MCP Docker Container

```bash
agenticai mcp run <mcp-server-dir-name> --port 8000 --tag local
# Options:
#   --port <port>        # Host port to expose (default: 8000)
#   --tag <name>         # Docker image tag (default: local)
#   --env-file <path>    # Path to .env file (default: <mcp-dir>/.env)
#   --detach/--foreground# Run in background or foreground
```

### 3. Stopping, Logs, and Status

```bash
agenticai mcp stop <mcp-server-dir-name>
agenticai mcp logs <mcp-server-dir-name> --follow
agenticai mcp status
```

---

## Accessing the MCP Server

Once running, access the MCP server at:

```
http://localhost:8000
```

You can test endpoints, health, and tool APIs using curl, Postman, or your LLM/agent client.

---

## Running Locally (Without Docker)

1. **Install dependencies:**
   ```bash
   pip install -e .
   ```
2. **Start the server:**
   ```bash
   python -m src.main
   # or for HTTP transport:
   python -c "from src.main import mcp; mcp.run(transport='http', host=8000)"
   ```

---

## Deployment to Azure (Terraform)

1. **Configure terraform.tfvars:**
   ```bash
   cp terraform/terraform.tfvars.example terraform/terraform.tfvars
   # Edit with your Azure subscription, resource group, registry, etc.
   ```
2. **Build and push Docker image:**
   ```bash
   agenticai mcp build <mcp-server-dir-name> --tag v1.0.0 --workspace <path>
   # Tag and push to your registry as needed
   ```
3. **Deploy infrastructure:**
   ```bash
   agenticai mcp deploy <mcp-server-dir-name> --subscription-id <sub-id> --container-image <image> --state-rg <state-rg> --state-storage <state-storage>
   ```
4. **Get outputs:**
   ```bash
   agenticai mcp status
   # Or use the CLI or Terraform output to get the public URL
   ```

---

## Further Reading

- [Model Context Protocol](https://modelcontextprotocol.io/)
- [FastMCP Framework](https://github.com/jlowin/fastmcp)
- [agenticai CLI](https://pypi.org/project/agenticai/)
- See the `docs/` directory for advanced CLI, deployment, and integration guides.

---

## Contact & Support

For questions, issues, or contributions, please refer to the repository's issue tracker or contact the maintainers listed in the main README.

---

*This documentation is intended to provide a complete, general-purpose guide for developers, DevOps, and contributors working with the Agentic AI MCP Server template.*
