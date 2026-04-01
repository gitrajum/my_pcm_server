# Development Guide

## Setup Development Environment

### Prerequisites
- Python 3.11+
- pip
- Virtual environment (recommended)

### Installation

```bash
# Clone repository
git clone <repository-url>
cd grafana-mcp-server

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows
.venv\Scripts\activate
# Linux/Mac
source .venv/bin/activate

# Install in development mode with all extras
pip install -e ".[dev,auth,telemetry]"
```

## Development Workflow

### 1. Create Feature Branch

```bash
git checkout -b feature/my-new-tool
```

### 2. Implement Changes

- Add/modify code in `src/`
- Follow existing patterns
- Use type hints
- Add docstrings

### 3. Write Tests

```bash
# tests/test_my_tool.py
import pytest

@pytest.mark.unit
@pytest.mark.asyncio
async def test_my_tool():
    # Test implementation
    pass
```

### 4. Run Tests

```bash
# All tests
pytest

# Watch mode
pytest-watch

# With coverage
pytest --cov=src --cov-report=html
open htmlcov/index.html
```

### 5. Code Quality

```bash
# Format code
black src/ tests/

# Lint
ruff check src/ tests/ --fix

# Type check
mypy src/
```

### 6. Manual Testing

```bash
# Run server locally
python -m src.main

# Or use the entry point
grafana-mcp-server

# Test with MCP Inspector
npx @modelcontextprotocol/inspector grafana-mcp-server
```

### 7. Commit and Push

```bash
git add .
git commit -m "feat: add my new tool"
git push origin feature/my-new-tool
```

## Project Structure

```
src/
├── main.py              # Entry point, server setup
├── config.py            # Configuration management
└── services/            # Tool services
    ├── __init__.py
    ├── hello_world_service.py
    ├── auth_service.py
    └── telemetry_service.py

tests/
├── conftest.py          # Pytest configuration
├── test_hello_world_service.py
└── test_server.py

docs/
├── architecture.md      # Architecture documentation
└── development.md       # This file
```

## Adding a New Tool Service

1. **Create Service Module**

```python
# src/services/calculator_service.py
from fastmcp import FastMCP
import logging

logger = logging.getLogger(__name__)

def register_calculator_tools(mcp: FastMCP):
    @mcp.tool()
    async def multiply(a: float, b: float) -> float:
        """Multiply two numbers."""
        logger.info(f"multiply called: {a} * {b}")
        return a * b

    logger.info("Calculator tools registered")
```

2. **Register in Main**

```python
# src/main.py
from .services.calculator_service import register_calculator_tools

register_calculator_tools(mcp)
```

3. **Add Tests**

```python
# tests/test_calculator_service.py
import pytest
from src.services.calculator_service import register_calculator_tools
from fastmcp import FastMCP

@pytest.mark.unit
@pytest.mark.asyncio
async def test_multiply():
    mcp = FastMCP(name="test")
    register_calculator_tools(mcp)
    
    result = await mcp.call_tool("multiply", {"a": 3, "b": 4})
    assert result == 12
```

4. **Update Documentation**

Add tool documentation to README.md.

## Debugging

### VS Code Launch Configuration

```json
// .vscode/launch.json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Run MCP Server",
      "type": "python",
      "request": "launch",
      "module": "src.main",
      "console": "integratedTerminal",
      "justMyCode": true
    },
    {
      "name": "Run Tests",
      "type": "python",
      "request": "launch",
      "module": "pytest",
      "args": ["-v"],
      "console": "integratedTerminal"
    }
  ]
}
```

### Logging

```python
import logging

logger = logging.getLogger(__name__)

# In tools
logger.debug("Debug information")
logger.info("Tool called with params: %s", params)
logger.warning("Warning message")
logger.error("Error occurred: %s", error, exc_info=True)
```

### MCP Inspector

Test your server with the official MCP Inspector:

```bash
npx @modelcontextprotocol/inspector grafana-mcp-server
```

## Testing Strategy

### Unit Tests
- Test individual tools
- Mock external dependencies
- Fast execution

### Integration Tests
- Test server initialization
- Test tool registration
- Test configuration loading

### Test Markers

```python
@pytest.mark.unit        # Unit test
@pytest.mark.integration # Integration test
@pytest.mark.asyncio     # Async test
```

## Common Issues

### Import Errors
- Ensure virtual environment is activated
- Check `sys.path` includes `src/`

### Tool Not Found
- Verify tool is registered in `main.py`
- Check tool name matches

### Configuration Not Loading
- Verify `.env` file exists
- Check environment variable names
- Use `get_settings()` to debug

## Release Process

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Run full test suite
4. Tag release: `git tag v1.0.0`
5. Push: `git push --tags`
6. Build: `python -m build`
7. Publish: `twine upload dist/*`

## Resources

- [FastMCP Documentation](https://github.com/jlowin/fastmcp)
- [MCP Specification](https://spec.modelcontextprotocol.io/)
- [Pytest Documentation](https://docs.pytest.org/)
- [Pydantic Documentation](https://docs.pydantic.dev/)
