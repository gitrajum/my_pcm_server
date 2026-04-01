# MCP Server Template - Quick Start Guide

## Create Your MCP Server in 5 Minutes

### 1. Scaffold the Project

```bash
agenticai scaffold my-mcp-server --template mcp_server
```

Interactive prompts will ask for:
- Description
- Author name and email
- Port (default: 3000)

**Non-interactive**:
```bash
agenticai scaffold my-mcp-server -t mcp_server --no-interactive \
  --description "My awesome MCP server" \
  --author-name "Your Name" \
  --author-email "you@example.com" \
  --port 3000
```

### 2. Install Dependencies

```bash
cd my-mcp-server
pip install -e .
```

**With optional features**:
```bash
# Authentication support
pip install -e ".[auth]"

# Telemetry support  
pip install -e ".[telemetry]"

# Everything
pip install -e ".[dev,auth,telemetry]"
```

### 3. Configure Environment

```bash
cp .env.example .env
```

Edit `.env`:
```env
MCP_SERVER_HOST=0.0.0.0
MCP_SERVER_PORT=3000
LOG_LEVEL=INFO
```

### 4. Run the Server

```bash
# Run the server (HTTP mode on port 3000)
my-mcp-server

# Or with Python
python -m src.main
```

### 5. Test with MCP Inspector

```bash
npx @modelcontextprotocol/inspector my-mcp-server
```

## Adding Your First Custom Tool

### 1. Create a Service Module

Create `src/services/my_service.py`:

```python
"""My custom service with tools."""

import logging
from fastmcp import FastMCP

logger = logging.getLogger(__name__)

def register_my_tools(mcp: FastMCP):
    """Register my custom tools."""
    
    @mcp.tool()
    async def my_tool(input_text: str) -> str:
        """
        Description of what my tool does.
        
        Args:
            input_text: Description of the parameter
            
        Returns:
            Description of the return value
        """
        logger.info(f"my_tool called with: {input_text}")
        
        # Your logic here
        result = f"Processed: {input_text}"
        
        return result
    
    logger.info("My tools registered")
```

### 2. Register the Service

Edit `src/main.py`:

```python
# Add import at top
from .services.my_service import register_my_tools

# Register after existing services
register_my_tools(mcp)
```

### 3. Test Your Tool

```python
# Test locally
cd my-mcp-server
python -c "from src.main import mcp; print('✓ Tools:', list(mcp.get_tools()))"
```

## Project Structure Reference

```
my-mcp-server/
├── src/
│   ├── main.py              # MCP server - edit to add services
│   ├── config.py            # Configuration - edit for new settings
│   └── services/
│       ├── hello_world_service.py   # Example - study this
│       └── my_service.py            # Your new service
├── tests/
│   ├── test_hello_world_service.py  # Example tests
│   └── test_my_service.py           # Your tests
├── .env                     # Your configuration (gitignored)
├── README.md               # Documentation
└── pyproject.toml          # Dependencies
```

## Common Tasks

### Add a Resource

```python
@mcp.resource("data://my-resource")
def get_my_resource() -> str:
    """Get some data as a resource."""
    return "Resource data here"
```

### Add a Prompt Template

```python
@mcp.prompt("my-prompt")
def my_prompt(topic: str) -> str:
    """Generate a prompt for a specific topic."""
    return f"Please analyze the following topic: {topic}"
```

### Enable Authentication

1. Install auth extras: `pip install -e ".[auth]"`

2. Configure `.env`:
```env
MCP_AUTH_ENABLED=true
AUTH_RESOURCE_SERVER_URL=https://login.microsoftonline.com/{tenant}/v2.0
AUTH_CLIENT_ID=your-client-id
AUTH_AUDIENCE=api://your-api-id
```

3. Pass Bearer token in requests

### Enable Telemetry

1. Install telemetry extras: `pip install -e ".[telemetry]"`

2. Configure `.env`:
```env
OTEL_ENABLED=true
OTEL_SERVICE_NAME=my-mcp-server
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

3. Run with telemetry collector

### Run Tests

```bash
# All tests
pytest

# Unit tests only
pytest -m unit

# Integration tests only
pytest -m integration

# With coverage
pytest --cov=src --cov-report=html
```

### Debug in VS Code

1. Open folder in VS Code
2. Press F5
3. Choose "Run MCP Server (stdio)" or "Run MCP Server (HTTP)"
4. Set breakpoints and debug

### Docker Deployment

```bash
# Build
docker build -t my-mcp-server:latest .

# Run
docker run --rm -it \
  -p 3000:3000 \
  --env-file .env \
  my-mcp-server:latest
```

### Azure Deployment

Deploy to Azure Container Apps:

```bash
# 1. Install CLI dependencies
pip install -e ".[cli]"

# 2. Build and push image
my-mcp-server-cli docker build --tag v1.0.0
my-mcp-server-cli docker push --tag v1.0.0 --registry myregistry.azurecr.io

# 3. Configure terraform/terraform.tfvars
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform.tfvars with your Azure settings

# 4. Deploy infrastructure
my-mcp-server-cli iac deploy \
  --subscription-id <your-subscription-id> \
  --container-image myregistry.azurecr.io/my-mcp-server:v1.0.0 \
  --state-rg rg-terraform-state \
  --state-storage stterraformstate

# 5. Get deployment URL
my-mcp-server-cli iac output
```

See [DEPLOYMENT.md](DEPLOYMENT.md) for complete deployment guide.

## Tool Development Tips

### 1. Use Type Hints
```python
async def my_tool(count: int, name: str) -> dict:
    """Types help with validation and documentation."""
    return {"count": count, "name": name}
```

### 2. Handle Errors Gracefully
```python
@mcp.tool()
async def safe_tool(value: str) -> str:
    """Tool with error handling."""
    try:
        result = process(value)
        return result
    except ValueError as e:
        logger.error(f"Error processing {value}: {e}")
        return f"Error: Invalid input - {e}"
```

### 3. Log Extensively
```python
logger.debug("Detailed debug information")
logger.info("Tool called with params: %s", params)
logger.warning("Something unusual happened")
logger.error("Error occurred", exc_info=True)
```

### 4. Write Tests
```python
@pytest.mark.unit
@pytest.mark.asyncio
async def test_my_tool():
    """Test my_tool function."""
    from src.services.my_service import register_my_tools
    from fastmcp import FastMCP
    
    mcp = FastMCP(name="test")
    register_my_tools(mcp)
    
    result = await mcp.call_tool("my_tool", {"input_text": "test"})
    assert "Processed" in result
```

### 5. Document Thoroughly
```python
@mcp.tool()
async def well_documented_tool(
    required_param: str,
    optional_param: int = 10
) -> dict:
    """
    One-line summary of what the tool does.
    
    More detailed explanation of the tool's purpose,
    behavior, and any important notes.
    
    Args:
        required_param: Description of this parameter
        optional_param: Description with default value
        
    Returns:
        Dictionary with keys 'result' and 'status'
        
    Example:
        >>> tool("hello", 5)
        {"result": "...", "status": "success"}
    """
    pass
```

## Troubleshooting

### "Module not found" errors
```bash
# Make sure you're in the project directory
cd my-mcp-server

# Reinstall in editable mode
pip install -e .
```

### "Tool not found" errors
- Check tool is registered in `main.py`
- Verify service function is called
- Check for typos in tool name

### Server won't start
- Check `.env` file exists
- Verify port is not in use
- Check logs for error details: `LOG_LEVEL=DEBUG`

### Authentication issues
- Verify `MCP_AUTH_ENABLED=true` in `.env`
- Check auth dependencies installed: `pip install -e ".[auth]"`
- Validate token format (must be `Bearer <token>`)

## Next Steps

1. **Read Full Documentation**: See README.md
2. **Study Architecture**: See docs/architecture.md
3. **Learn Development Workflow**: See docs/development.md
4. **Explore Example Services**: Check src/services/
5. **Write Tests**: Add to tests/
6. **Deploy**: Use Docker or your preferred method

## Getting Help

- **Template Issues**: Check MCP_TEMPLATE_IMPLEMENTATION_SUMMARY.md
- **FastMCP Documentation**: https://github.com/jlowin/fastmcp
- **MCP Specification**: https://spec.modelcontextprotocol.io/

---

**Happy MCP Server Building! 🚀**
