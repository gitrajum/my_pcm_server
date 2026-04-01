# MCP Server Template Documentation

Welcome to the comprehensive documentation for the MCP Server Template - a production-ready template for building Model Context Protocol (MCP) servers using FastMCP.

## 📚 Documentation Structure

### 🚀 [Quick Start](guides/quick-start.md)
Get your MCP server running in minutes with minimal setup.

### 📖 [User Guides](guides/README.md)
Comprehensive guides covering all aspects of MCP server development.

### 🏗️ [Architecture](architecture/README.md)
Server architecture, design patterns, and component structure.

### 📘 [API Reference](api-reference/README.md)
Complete API documentation for tools, resources, and prompts.

### 💡 [Examples](examples/README.md)
Practical examples and usage patterns for common scenarios.

### 🤝 [Contributing](contributing/README.md)
Guidelines for contributing to the template.

## Quick Links

### Getting Started
- [**Quick Start**](guides/quick-start.md) - Installation and first steps
- [**Development Guide**](guides/development.md) - Local development workflow
- [**Configuration**](guides/configuration.md) - Server configuration
- [**Deployment**](guides/deployment.md) - Production deployment

### Core Concepts
- [**Tools**](guides/tools.md) - Creating and managing MCP tools
- [**Resources**](guides/resources.md) - Exposing server resources
- [**Prompts**](guides/prompts.md) - Defining prompt templates
- [**Authentication**](guides/authentication.md) - Security and auth setup

### Advanced Topics
- [**Telemetry**](guides/telemetry.md) - OpenTelemetry integration
- [**CLI Integration**](guides/cli.md) - Command-line interface
- [**Testing**](guides/testing.md) - Testing strategies
- [**Docker Deployment**](guides/docker.md) - Containerized deployment

## 📦 Installation

```bash
# Basic installation
pip install -e .

# With authentication support
pip install -e ".[auth]"

# With telemetry support
pip install -e ".[telemetry]"

# With all features
pip install -e ".[auth,telemetry]"

# With development tools
pip install -e ".[dev]"
```

## 🎯 Quick Example

### Basic MCP Server

```python
from fastmcp import FastMCP

# Create MCP server
mcp = FastMCP("My MCP Server")

# Define a tool
@mcp.tool()
async def hello(name: str) -> str:
    """Say hello to someone."""
    return f"Hello, {name}!"

# Define a resource
@mcp.resource("config://settings")
async def get_settings():
    """Get server settings."""
    return {"version": "1.0.0", "status": "active"}

# Define a prompt
@mcp.prompt()
async def welcome_prompt():
    """Welcome message prompt."""
    return "Welcome to the MCP server! How can I help you today?"

# Run server
if __name__ == "__main__":
    mcp.run()
```

### Running the Server

```bash
# Development mode
python -m src.main

# With CLI
mcp-server run

# With custom config
mcp-server run --config config.yaml

# In Docker
docker build -t my-mcp-server .
docker run -p 8000:8000 my-mcp-server
```

## 🏗️ Core Features

### FastMCP Framework
Built on [FastMCP](https://github.com/jlowin/fastmcp) for:
- **Lightweight**: Minimal overhead, maximum performance
- **Pythonic**: Familiar decorators and async/await
- **Type-safe**: Full Pydantic validation
- **Standards-compliant**: Full MCP protocol support

### Tools
Expose functionality to LLM applications:

```python
@mcp.tool()
async def calculate(operation: str, x: float, y: float) -> float:
    """Perform mathematical operations."""
    operations = {
        "add": x + y,
        "subtract": x - y,
        "multiply": x * y,
        "divide": x / y if y != 0 else float('inf')
    }
    return operations.get(operation, 0.0)
```

### Resources
Provide access to server data:

```python
@mcp.resource("data://users/{user_id}")
async def get_user(user_id: str):
    """Get user information."""
    return await database.get_user(user_id)
```

### Prompts
Define reusable prompt templates:

```python
@mcp.prompt()
async def analysis_prompt(topic: str):
    """Analysis prompt template."""
    return f"""Analyze the following topic: {topic}
    
Consider:
- Key concepts
- Implications
- Recommendations
"""
```

### Optional Features

#### Authentication
JWT-based authentication with OAuth2/Azure AD:

```python
from src.config import Settings

settings = Settings()
if settings.auth_enabled:
    mcp.add_auth(
        secret_key=settings.secret_key,
        token_url=settings.token_url
    )
```

#### Telemetry
OpenTelemetry instrumentation:

```python
from src.config import Settings

settings = Settings()
if settings.telemetry_enabled:
    mcp.add_telemetry(
        service_name="my-mcp-server",
        otlp_endpoint=settings.otlp_endpoint
    )
```

## 📋 Features Overview

| Feature | Description | Status |
|---------|-------------|--------|
| **Tools** | Expose functions to LLM | ✅ Ready |
| **Resources** | Provide data access | ✅ Ready |
| **Prompts** | Define templates | ✅ Ready |
| **Authentication** | JWT/OAuth2 auth | ⚙️ Optional |
| **Telemetry** | OpenTelemetry | ⚙️ Optional |
| **CLI** | Command-line interface | ✅ Ready |
| **Docker** | Container support | ✅ Ready |
| **Testing** | Unit & integration tests | ✅ Ready |

## 🔧 Configuration

### Environment Variables

```env
# Server Configuration
MCP_SERVER_NAME=my-mcp-server
MCP_SERVER_VERSION=1.0.0
MCP_PORT=8000
MCP_HOST=0.0.0.0

# Authentication (Optional)
AUTH_ENABLED=false
SECRET_KEY=your-secret-key
TOKEN_URL=https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token

# Telemetry (Optional)
TELEMETRY_ENABLED=false
OTLP_ENDPOINT=http://localhost:4317
SERVICE_NAME=my-mcp-server

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

### Configuration File

```yaml
# config.yaml
server:
  name: my-mcp-server
  version: 1.0.0
  port: 8000
  host: 0.0.0.0

auth:
  enabled: false
  secret_key: ${SECRET_KEY}
  token_url: ${TOKEN_URL}

telemetry:
  enabled: false
  otlp_endpoint: ${OTLP_ENDPOINT}
  service_name: ${SERVICE_NAME}

logging:
  level: INFO
  format: json
```

## 🐛 Troubleshooting

### Common Issues

**Server won't start:**
```bash
# Check configuration
mcp-server validate

# Check port availability
netstat -an | findstr :8000
```

**Authentication errors:**
```bash
# Verify environment variables
echo $SECRET_KEY
echo $TOKEN_URL

# Test token generation
mcp-server test-auth
```

**Import errors:**
```bash
# Reinstall dependencies
pip install -e ".[dev]"
```

## 📚 Additional Resources

- [FastMCP Documentation](https://github.com/jlowin/fastmcp)
- [MCP Protocol Specification](https://modelcontextprotocol.io/docs)
- [AgenticAI SDK](../../agentic_ai_sdk/docs/README.md)
- [Template Customization](guides/customization.md)

## 🆘 Getting Help

### Documentation
- [User Guides](guides/README.md) - Comprehensive how-to guides
- [API Reference](api-reference/README.md) - Complete API docs
- [Examples](examples/README.md) - Code examples

### CLI Help
```bash
# Get help
mcp-server --help

# Command-specific help
mcp-server run --help
mcp-server validate --help

# Check server status
mcp-server status
```

### Testing
```bash
# Run all tests
pytest

# Run specific test suite
pytest tests/test_tools.py

# Run with coverage
pytest --cov=src
```

## 🔄 Next Steps

1. **New Users**: Start with [Quick Start](guides/quick-start.md)
2. **Creating Tools**: Read [Tools Guide](guides/tools.md)
3. **Development**: Check [Development Guide](guides/development.md)
4. **Production**: Review [Deployment Guide](guides/deployment.md)
5. **Contributing**: See [Contributing Guide](contributing/README.md)

## 🎯 Use Cases

### LLM Tool Integration
Provide tools for LLM applications to call:
- Data retrieval and manipulation
- External API integration
- Complex calculations
- File operations

### Resource Exposure
Make data accessible to LLM applications:
- Configuration and settings
- Database queries
- File content
- API responses

### Prompt Templates
Define reusable prompts for:
- Analysis tasks
- Code generation
- Documentation
- Decision support

## 🚀 Deployment Options

### Local Development
```bash
python -m src.main
```

### Docker Container
```bash
docker build -t my-mcp-server .
docker run -p 8000:8000 my-mcp-server
```

### Kubernetes
```bash
kubectl apply -f k8s/deployment.yaml
```

### Azure Container Apps
```bash
az containerapp up \
  --name my-mcp-server \
  --image myregistry.azurecr.io/my-mcp-server:latest \
  --resource-group my-rg \
  --environment my-env
```

## 📊 Monitoring

### Health Checks
```bash
# Health endpoint
curl http://localhost:8000/health

# Readiness endpoint
curl http://localhost:8000/ready

# Metrics endpoint
curl http://localhost:8000/metrics
```

### Telemetry
When enabled, metrics and traces are exported to:
- **Traces**: OTLP endpoint
- **Metrics**: Prometheus format
- **Logs**: Structured JSON

## 🔐 Security

### Best Practices
- Use authentication in production
- Rotate secrets regularly
- Validate all inputs
- Limit resource access
- Monitor for anomalies

### Authentication Setup
See [Authentication Guide](guides/authentication.md) for detailed setup instructions.

## 📈 Performance

### Optimization Tips
- Use async/await for I/O operations
- Implement caching where appropriate
- Limit resource payload sizes
- Use connection pooling
- Monitor memory usage

### Scaling
- Horizontal scaling with load balancer
- Stateless design for easy scaling
- Resource limits in containers
- Auto-scaling based on load

## 🎓 Learning Path

1. **Beginner**: [Quick Start](guides/quick-start.md) → [Tools Guide](guides/tools.md)
2. **Intermediate**: [Development Guide](guides/development.md) → [Testing Guide](guides/testing.md)
3. **Advanced**: [Architecture](architecture/README.md) → [Customization](guides/customization.md)
4. **Production**: [Deployment](guides/deployment.md) → [Monitoring](guides/monitoring.md)
