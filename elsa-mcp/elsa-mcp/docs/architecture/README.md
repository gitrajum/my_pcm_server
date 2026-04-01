# ElsaMcp Architecture

## Overview

This MCP server follows a service-oriented architecture pattern where each service registers tools with the FastMCP server.

## Components

### Main Application (`main.py`)
- Initializes FastMCP server
- Registers all services
- Manages lifecycle (startup/shutdown)
- Provides global resources and prompts

### Configuration (`config.py`)
- Pydantic-based settings management
- Environment variable loading
- Centralized configuration access

### Services Layer (`services/`)
Each service module:
- Defines related tools as functions
- Registers tools with MCP server
- Handles business logic
- Manages service-specific state

### Optional Services
- **Authentication**: JWT validation, middleware
- **Telemetry**: OpenTelemetry tracing, metrics

## Communication Flow

```
MCP Client
    ↓
FastMCP Server (main.py)
    ↓
Tool Router
    ↓
Service Functions (services/)
    ↓
Business Logic
    ↓
Response
```

## Adding New Tools

1. Create a new service module in `services/`
2. Define tools using `@mcp.tool()` decorator
3. Register service in `main.py`
4. Add tests in `tests/`
5. Update README with tool documentation

Example:

```python
# services/my_service.py
def register_my_tools(mcp: FastMCP):
    @mcp.tool()
    async def my_tool(param: str) -> str:
        return f"Result: {param}"
```

```python
# main.py
from .services.my_service import register_my_tools

register_my_tools(mcp)
```

## Configuration Management

Settings are loaded in this order:
1. Default values in `config.py`
2. `.env` file
3. Environment variables (highest priority)

## Security Considerations

- Sensitive credentials in environment variables
- Optional JWT authentication
- Input validation with Pydantic
- Logging (excluding sensitive data)

## Performance

- Async/await for I/O operations
- Connection pooling (if needed)
- Caching (if appropriate)
- Resource cleanup in lifespan

## Monitoring

With telemetry enabled:
- Request/response tracing
- Tool execution metrics
- Error tracking
- Performance monitoring
