# MCP Server API Reference

Complete API reference for all MCP server components, tools, resources, and utilities.

## 📚 API Documentation

### Core Components
- [**Server**](core/server.md) - MCP server initialization and configuration
- [**Tools**](core/tools.md) - Tool registration and execution
- [**Resources**](core/resources.md) - Resource management and access
- [**Prompts**](core/prompts.md) - Prompt template definition

### Services
- [**Service Layer**](services/README.md) - Business logic organization
- [**Example Service**](services/hello-world.md) - Sample service implementation

### Configuration
- [**Settings**](config/settings.md) - Configuration management with Pydantic
- [**Environment Variables**](config/environment.md) - Environment configuration

### CLI
- [**Commands**](cli/commands.md) - CLI command reference
- [**Configuration**](cli/config.md) - CLI configuration options

### Utilities
- [**Helpers**](utilities/helpers.md) - Utility functions
- [**Validation**](utilities/validation.md) - Input validation
- [**Logging**](utilities/logging.md) - Logging utilities

## 🎯 Quick Reference

### Server Initialization

```python
from fastmcp import FastMCP

# Basic server
mcp = FastMCP("My MCP Server")

# With configuration
from src.config import Settings

settings = Settings()
mcp = FastMCP(
    name=settings.server_name,
    version=settings.server_version
)
```

### Tool Definition

```python
@mcp.tool()
async def my_tool(param: str) -> str:
    """
    Tool description.
    
    Args:
        param: Parameter description
        
    Returns:
        Result description
    """
    return f"Result: {param}"
```

### Resource Definition

```python
@mcp.resource("data://resource/{id}")
async def get_resource(id: str):
    """
    Resource description.
    
    Args:
        id: Resource identifier
        
    Returns:
        Resource data
    """
    return {"id": id, "data": "..."}
```

### Prompt Definition

```python
@mcp.prompt()
async def my_prompt(context: str):
    """
    Prompt description.
    
    Args:
        context: Context for prompt
        
    Returns:
        Prompt text
    """
    return f"Context: {context}\nPrompt: ..."
```

## 📦 Module Reference

### src.main

Server entry point and initialization.

**Functions:**
- `create_server() -> FastMCP`: Create and configure MCP server
- `main()`: Run server

### src.config

Configuration management with Pydantic.

**Classes:**
- `Settings`: Server configuration with validation
  - `server_name: str`
  - `server_version: str`
  - `port: int`
  - `host: str`
  - `auth_enabled: bool`
  - `telemetry_enabled: bool`

### src.services

Business logic services.

**Modules:**
- `hello_world`: Example service implementation
- `base`: Base service class

## 🔍 Detailed API

### Settings Class

Complete configuration management with Pydantic.

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Server configuration settings."""
    
    # Server
    server_name: str = "MCP Server"
    server_version: str = "1.0.0"
    port: int = 8000
    host: str = "0.0.0.0"
    
    # Authentication
    auth_enabled: bool = False
    secret_key: str = ""
    token_url: str = ""
    
    # Telemetry
    telemetry_enabled: bool = False
    otlp_endpoint: str = ""
    service_name: str = ""
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "json"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
```

### Tool Decorator

Define tools that can be called by LLM applications.

```python
def tool(
    name: Optional[str] = None,
    description: Optional[str] = None,
    **kwargs
) -> Callable:
    """
    Decorator for defining MCP tools.
    
    Args:
        name: Tool name (defaults to function name)
        description: Tool description (defaults to docstring)
        **kwargs: Additional tool metadata
        
    Returns:
        Decorated function registered as MCP tool
    """
```

**Usage:**
```python
@mcp.tool(name="calculate", description="Perform calculation")
async def calculate(x: float, y: float) -> float:
    return x + y
```

### Resource Decorator

Define resources that can be accessed by LLM applications.

```python
def resource(
    uri_template: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    **kwargs
) -> Callable:
    """
    Decorator for defining MCP resources.
    
    Args:
        uri_template: Resource URI template (e.g., "data://users/{id}")
        name: Resource name (defaults to function name)
        description: Resource description (defaults to docstring)
        **kwargs: Additional resource metadata
        
    Returns:
        Decorated function registered as MCP resource
    """
```

**Usage:**
```python
@mcp.resource("data://users/{user_id}")
async def get_user(user_id: str):
    return await db.get_user(user_id)
```

### Prompt Decorator

Define prompt templates.

```python
def prompt(
    name: Optional[str] = None,
    description: Optional[str] = None,
    **kwargs
) -> Callable:
    """
    Decorator for defining MCP prompts.
    
    Args:
        name: Prompt name (defaults to function name)
        description: Prompt description (defaults to docstring)
        **kwargs: Additional prompt metadata
        
    Returns:
        Decorated function registered as MCP prompt
    """
```

**Usage:**
```python
@mcp.prompt(name="analysis", description="Analysis prompt template")
async def analysis_prompt(topic: str):
    return f"Analyze: {topic}"
```

## 📊 Data Models

### ToolMetadata

```python
@dataclass
class ToolMetadata:
    name: str
    description: str
    parameters: Dict[str, ParameterMetadata]
    returns: TypeMetadata
```

### ResourceMetadata

```python
@dataclass
class ResourceMetadata:
    uri_template: str
    name: str
    description: str
    mime_type: str
```

### PromptMetadata

```python
@dataclass
class PromptMetadata:
    name: str
    description: str
    parameters: Dict[str, ParameterMetadata]
```

## 🔧 CLI Reference

### Commands

```bash
# Run server
mcp-server run [OPTIONS]

Options:
  --config PATH    Configuration file path
  --port INTEGER   Server port (default: 8000)
  --host TEXT      Server host (default: 0.0.0.0)
  --reload         Enable auto-reload

# Validate configuration
mcp-server validate [OPTIONS]

Options:
  --config PATH    Configuration file path

# Test authentication
mcp-server test-auth [OPTIONS]

Options:
  --token TEXT     JWT token to test

# Show server status
mcp-server status
```

## 🛠️ Utility Functions

### Logging

```python
from src.utils.logging import get_logger

logger = get_logger(__name__)
logger.info("Message")
logger.error("Error", exc_info=True)
```

### Validation

```python
from src.utils.validation import validate_input

@validate_input
async def my_tool(param: str) -> str:
    return f"Valid: {param}"
```

## 📋 Error Handling

### Exception Hierarchy

```python
class MCPError(Exception):
    """Base MCP exception."""

class ToolError(MCPError):
    """Tool execution error."""

class ResourceError(MCPError):
    """Resource access error."""

class AuthenticationError(MCPError):
    """Authentication error."""

class ConfigurationError(MCPError):
    """Configuration error."""
```

### Error Handling Example

```python
from src.exceptions import ToolError

@mcp.tool()
async def my_tool(param: str) -> str:
    try:
        result = await some_operation(param)
        return result
    except ValueError as e:
        raise ToolError(f"Invalid parameter: {e}")
    except Exception as e:
        logger.error("Unexpected error", exc_info=True)
        raise ToolError(f"Operation failed: {e}")
```

## 📚 Related Documentation

- [User Guides](../guides/README.md) - How-to guides
- [Architecture](../architecture/README.md) - System architecture
- [Examples](../examples/README.md) - Code examples
- [FastMCP Documentation](https://github.com/jlowin/fastmcp) - Framework docs
