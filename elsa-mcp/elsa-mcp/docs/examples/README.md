# MCP Server Examples

This guide provides practical examples for building MCP servers with FastMCP.

## Quick Examples

### 1. Simple Calculator Tool

```python
from fastmcp import FastMCP

mcp = FastMCP("Calculator")

@mcp.tool()
def add(a: float, b: float) -> float:
    """Add two numbers together."""
    return a + b

@mcp.tool()
def multiply(a: float, b: float) -> float:
    """Multiply two numbers."""
    return a * b

if __name__ == "__main__":
    mcp.run()
```

**Usage:**
```bash
uv run calculator.py
```

### 2. File System Operations

```python
from fastmcp import FastMCP, Context
from pathlib import Path
from typing import Annotated

mcp = FastMCP("FileSystem")

@mcp.tool()
def read_file(
    path: Annotated[str, "Path to the file to read"],
    ctx: Context
) -> str:
    """Read contents of a file."""
    file_path = Path(path)
    
    if not file_path.exists():
        raise ValueError(f"File not found: {path}")
    
    ctx.info(f"Reading file: {path}")
    return file_path.read_text()

@mcp.tool()
def write_file(
    path: Annotated[str, "Path to the file to write"],
    content: Annotated[str, "Content to write"],
    ctx: Context
) -> str:
    """Write content to a file."""
    file_path = Path(path)
    file_path.write_text(content)
    
    ctx.info(f"Wrote {len(content)} characters to {path}")
    return f"Successfully wrote to {path}"

if __name__ == "__main__":
    mcp.run()
```

### 3. HTTP API Integration

```python
from fastmcp import FastMCP, Context
import httpx
from typing import Annotated
from pydantic import BaseModel

mcp = FastMCP("WeatherAPI")

class WeatherData(BaseModel):
    temperature: float
    condition: str
    humidity: int

@mcp.tool()
async def get_weather(
    city: Annotated[str, "City name"],
    ctx: Context
) -> WeatherData:
    """Get current weather for a city."""
    ctx.info(f"Fetching weather for {city}")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://api.weather.example.com/current",
            params={"city": city}
        )
        response.raise_for_status()
        data = response.json()
    
    return WeatherData(
        temperature=data["temp"],
        condition=data["condition"],
        humidity=data["humidity"]
    )

if __name__ == "__main__":
    mcp.run()
```

### 4. Dynamic Resources

```python
from fastmcp import FastMCP, Context
from typing import Annotated
import json

mcp = FastMCP("DataStore")

# In-memory data store
data_store = {}

@mcp.resource("data://{key}")
def get_data(
    key: Annotated[str, "Data key"],
    ctx: Context
) -> str:
    """Retrieve data by key."""
    if key not in data_store:
        raise ValueError(f"Key not found: {key}")
    
    ctx.info(f"Retrieved data for key: {key}")
    return json.dumps(data_store[key])

@mcp.tool()
def store_data(
    key: Annotated[str, "Data key"],
    value: Annotated[str, "Data value"],
    ctx: Context
) -> str:
    """Store data with a key."""
    data_store[key] = value
    ctx.info(f"Stored data for key: {key}")
    return f"Stored {key}"

@mcp.tool()
def list_keys(ctx: Context) -> list[str]:
    """List all stored keys."""
    keys = list(data_store.keys())
    ctx.info(f"Found {len(keys)} keys")
    return keys

if __name__ == "__main__":
    mcp.run()
```

### 5. Prompts with Templates

```python
from fastmcp import FastMCP, Context
from typing import Annotated

mcp = FastMCP("CodeReviewer")

@mcp.prompt()
def code_review(
    code: Annotated[str, "Code to review"],
    language: Annotated[str, "Programming language"] = "python",
    ctx: Context = None
) -> str:
    """Generate a code review prompt."""
    if ctx:
        ctx.info(f"Creating code review for {language}")
    
    return f"""Please review the following {language} code:

```{language}
{code}
```

Focus on:
1. Code quality and best practices
2. Potential bugs or issues
3. Performance improvements
4. Security concerns
5. Readability and maintainability

Provide specific suggestions for improvement."""

@mcp.prompt()
def refactor_suggestion(
    code: Annotated[str, "Code to refactor"],
    goal: Annotated[str, "Refactoring goal"],
    ctx: Context = None
) -> str:
    """Generate a refactoring prompt."""
    if ctx:
        ctx.info(f"Creating refactoring suggestion: {goal}")
    
    return f"""Refactor the following code to achieve: {goal}

Original code:
```
{code}
```

Provide:
1. Refactored code with explanations
2. Why this refactoring improves the code
3. Any trade-offs to consider"""

if __name__ == "__main__":
    mcp.run()
```

### 6. Authentication with JWT

```python
from fastmcp import FastMCP, Context
from src.config import Settings
from src.services.auth_service import AuthService
import jwt
from datetime import datetime, timedelta

mcp = FastMCP("SecureAPI")
settings = Settings()

# Initialize auth service if enabled
if settings.auth_enabled:
    auth_service = AuthService(settings)
    mcp.dependencies.append(lambda: {"auth": auth_service})

@mcp.tool()
def create_token(
    user_id: Annotated[str, "User identifier"],
    ctx: Context
) -> str:
    """Create a JWT token for a user."""
    if not settings.auth_enabled:
        return "Authentication is disabled"
    
    auth_service = ctx.request_context.get("auth")
    payload = {
        "user_id": user_id,
        "exp": datetime.utcnow() + timedelta(hours=24)
    }
    
    token = jwt.encode(payload, settings.jwt_secret, algorithm="HS256")
    ctx.info(f"Created token for user: {user_id}")
    return token

@mcp.tool()
def verify_token(
    token: Annotated[str, "JWT token to verify"],
    ctx: Context
) -> dict:
    """Verify a JWT token."""
    if not settings.auth_enabled:
        return {"valid": False, "message": "Authentication is disabled"}
    
    auth_service = ctx.request_context.get("auth")
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=["HS256"]
        )
        ctx.info(f"Token verified for user: {payload['user_id']}")
        return {"valid": True, "user_id": payload["user_id"]}
    except jwt.ExpiredSignatureError:
        return {"valid": False, "message": "Token expired"}
    except jwt.InvalidTokenError:
        return {"valid": False, "message": "Invalid token"}

if __name__ == "__main__":
    mcp.run()
```

### 7. Telemetry with OpenTelemetry

```python
from fastmcp import FastMCP, Context
from src.config import Settings
from src.services.telemetry_service import TelemetryService
from typing import Annotated
import time

mcp = FastMCP("MonitoredAPI")
settings = Settings()

# Initialize telemetry if enabled
if settings.telemetry_enabled:
    telemetry = TelemetryService(settings)
    mcp.dependencies.append(lambda: {"telemetry": telemetry})

@mcp.tool()
def process_data(
    data: Annotated[str, "Data to process"],
    ctx: Context
) -> str:
    """Process data with telemetry tracking."""
    if settings.telemetry_enabled:
        telemetry = ctx.request_context.get("telemetry")
        with telemetry.tracer.start_as_current_span("process_data") as span:
            span.set_attribute("data.length", len(data))
            
            start_time = time.time()
            # Simulate processing
            result = data.upper()
            duration = time.time() - start_time
            
            span.set_attribute("processing.duration", duration)
            telemetry.metrics["data_processed"].add(1)
            
            ctx.info(f"Processed {len(data)} characters in {duration:.3f}s")
            return result
    else:
        return data.upper()

if __name__ == "__main__":
    mcp.run()
```

### 8. Error Handling

```python
from fastmcp import FastMCP, Context
from typing import Annotated
from pydantic import BaseModel, ValidationError

mcp = FastMCP("ErrorHandling")

class UserData(BaseModel):
    username: str
    email: str
    age: int

@mcp.tool()
def validate_user(
    data: Annotated[dict, "User data to validate"],
    ctx: Context
) -> dict:
    """Validate user data with error handling."""
    try:
        user = UserData(**data)
        ctx.info(f"Valid user data for: {user.username}")
        return {
            "valid": True,
            "user": user.model_dump()
        }
    except ValidationError as e:
        ctx.error(f"Validation failed: {e}")
        return {
            "valid": False,
            "errors": [
                {
                    "field": err["loc"][0],
                    "message": err["msg"]
                }
                for err in e.errors()
            ]
        }

@mcp.tool()
def divide_numbers(
    a: Annotated[float, "Dividend"],
    b: Annotated[float, "Divisor"],
    ctx: Context
) -> float:
    """Divide two numbers with error handling."""
    try:
        if b == 0:
            raise ValueError("Cannot divide by zero")
        
        result = a / b
        ctx.info(f"Division result: {result}")
        return result
    except Exception as e:
        ctx.error(f"Division error: {e}")
        raise

if __name__ == "__main__":
    mcp.run()
```

## Common Patterns

### Dependency Injection

```python
from fastmcp import FastMCP, Context
from typing import Annotated

class DatabaseConnection:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def query(self, sql: str) -> list[dict]:
        # Simulated database query
        return [{"id": 1, "name": "Example"}]

mcp = FastMCP("Database")

# Register dependency
db = DatabaseConnection("postgresql://localhost/mydb")
mcp.dependencies.append(lambda: {"db": db})

@mcp.tool()
def query_database(
    sql: Annotated[str, "SQL query"],
    ctx: Context
) -> list[dict]:
    """Execute a database query."""
    db = ctx.request_context.get("db")
    results = db.query(sql)
    ctx.info(f"Query returned {len(results)} rows")
    return results
```

### Caching Results

```python
from fastmcp import FastMCP, Context
from functools import lru_cache
from typing import Annotated

mcp = FastMCP("Caching")

@lru_cache(maxsize=100)
def expensive_computation(n: int) -> int:
    """Simulate expensive computation."""
    result = sum(range(n))
    return result

@mcp.tool()
def compute(
    n: Annotated[int, "Input number"],
    ctx: Context
) -> int:
    """Perform expensive computation with caching."""
    ctx.info(f"Computing for n={n}")
    result = expensive_computation(n)
    ctx.info(f"Result: {result}")
    return result
```

### Streaming Responses

```python
from fastmcp import FastMCP, Context
from typing import Annotated, AsyncGenerator

mcp = FastMCP("Streaming")

@mcp.tool()
async def stream_data(
    count: Annotated[int, "Number of items to stream"],
    ctx: Context
) -> AsyncGenerator[str, None]:
    """Stream data items."""
    ctx.info(f"Starting stream of {count} items")
    
    for i in range(count):
        yield f"Item {i + 1}"
        ctx.debug(f"Streamed item {i + 1}")
```

### Configuration Management

```python
from fastmcp import FastMCP, Context
from pydantic_settings import BaseSettings
from typing import Annotated

class Settings(BaseSettings):
    api_key: str
    timeout: int = 30
    debug: bool = False
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

mcp = FastMCP("Config")
settings = Settings()

@mcp.tool()
def get_config(
    key: Annotated[str, "Configuration key"],
    ctx: Context
) -> str:
    """Get configuration value."""
    value = getattr(settings, key, None)
    if value is None:
        raise ValueError(f"Unknown config key: {key}")
    
    ctx.info(f"Retrieved config {key}: {value}")
    return str(value)
```

## Testing Examples

### Unit Testing Tools

```python
import pytest
from fastmcp import FastMCP, Context
from unittest.mock import Mock

def test_add_tool():
    """Test the add tool."""
    mcp = FastMCP("Calculator")
    
    @mcp.tool()
    def add(a: float, b: float) -> float:
        return a + b
    
    # Call the tool directly
    result = add(2, 3)
    assert result == 5

def test_tool_with_context():
    """Test tool with context."""
    mcp = FastMCP("Test")
    
    @mcp.tool()
    def process(data: str, ctx: Context) -> str:
        ctx.info(f"Processing: {data}")
        return data.upper()
    
    # Mock the context
    mock_ctx = Mock(spec=Context)
    result = process("hello", ctx=mock_ctx)
    
    assert result == "HELLO"
    mock_ctx.info.assert_called_once_with("Processing: hello")
```

### Integration Testing

```python
import pytest
from fastmcp import FastMCP
import httpx

@pytest.mark.asyncio
async def test_server_integration():
    """Test server integration."""
    # Start server in test mode
    mcp = FastMCP("TestServer")
    
    @mcp.tool()
    def hello() -> str:
        return "Hello, World!"
    
    # Test with httpx client
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8000/tools/hello",
            json={}
        )
        assert response.status_code == 200
        assert response.json()["result"] == "Hello, World!"
```

## Next Steps

- Review [API Reference](../api-reference/README.md) for detailed documentation
- Check [Development Guide](../guides/development.md) for best practices
- Explore [Architecture](../architecture/README.md) for system design
- See [Deployment Guide](../guides/deployment.md) for production setup

## Additional Resources

- [FastMCP Documentation](https://github.com/jlowin/fastmcp)
- [MCP Protocol Specification](https://spec.modelcontextprotocol.io)
- [Python Type Hints](https://docs.python.org/3/library/typing.html)
- [Pydantic Documentation](https://docs.pydantic.dev)
