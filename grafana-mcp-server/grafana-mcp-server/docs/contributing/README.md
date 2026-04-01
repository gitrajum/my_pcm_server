# Contributing to MCP Server Template

Thank you for your interest in contributing to the MCP Server Template! This guide will help you get started with development and contributions.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Code Standards](#code-standards)
- [Testing Guidelines](#testing-guidelines)
- [Documentation](#documentation)
- [Submitting Changes](#submitting-changes)
- [Release Process](#release-process)

## Getting Started

### Prerequisites

Before contributing, ensure you have:

- **Python 3.11+** installed
- **uv** package manager (`pip install uv`)
- **Git** for version control
- **Docker** (optional, for container testing)
- **Azure CLI** (optional, for cloud deployment testing)

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork locally:

```bash
git clone https://github.com/YOUR_USERNAME/agentic_ai_template_mcp_server.git
cd agentic_ai_template_mcp_server
```

3. Add upstream remote:

```bash
git remote add upstream https://github.com/bayer-int/agentic_ai_template_mcp_server.git
```

## Development Setup

### Install Dependencies

```bash
# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e ".[dev]"
```

### Configure Environment

1. Copy the example environment file:

```bash
cp .env.example .env
```

2. Update `.env` with your configuration:

```bash
# Server Configuration
SERVER_NAME="your-server-name"
LOG_LEVEL="INFO"

# Authentication (optional)
AUTH_ENABLED=false
JWT_SECRET="your-secret-key"

# Telemetry (optional)
TELEMETRY_ENABLED=false
OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"
```

### Run Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_server.py

# Run with verbose output
pytest -v
```

### Start Development Server

```bash
# Using uv
uv run src/main.py

# Using CLI
uv run cli/main.py run

# With debug logging
LOG_LEVEL=DEBUG uv run src/main.py
```

## Code Standards

### Python Style Guide

We follow **PEP 8** with some modifications:

- **Line length**: 88 characters (Black default)
- **Imports**: Use `isort` for consistent ordering
- **Type hints**: Required for all functions
- **Docstrings**: Google-style format

### Code Formatting

Format code before committing:

```bash
# Format with Black
black src/ tests/ cli/

# Sort imports with isort
isort src/ tests/ cli/

# Check with flake8
flake8 src/ tests/ cli/

# Run all formatters
black . && isort . && flake8 .
```

### Type Checking

Use **Pyright** for type checking:

```bash
# Check types
pyright

# Check specific directory
pyright src/
```

### Example: Well-Formatted Code

```python
from typing import Annotated

from fastmcp import Context, FastMCP
from pydantic import BaseModel


class UserData(BaseModel):
    """User data model.
    
    Attributes:
        username: The user's username
        email: The user's email address
        age: The user's age
    """
    
    username: str
    email: str
    age: int


mcp = FastMCP("UserService")


@mcp.tool()
def create_user(
    username: Annotated[str, "Username for the new user"],
    email: Annotated[str, "Email address"],
    age: Annotated[int, "User age"],
    ctx: Context,
) -> dict[str, str]:
    """Create a new user.
    
    Args:
        username: The desired username
        email: The user's email address
        age: The user's age in years
        ctx: Request context for logging
        
    Returns:
        Dictionary containing user creation status
        
    Raises:
        ValueError: If age is negative or username is empty
    """
    if age < 0:
        raise ValueError("Age cannot be negative")
    if not username.strip():
        raise ValueError("Username cannot be empty")
    
    ctx.info(f"Creating user: {username}")
    
    user = UserData(username=username, email=email, age=age)
    return {"status": "created", "user_id": "123"}
```

## Testing Guidelines

### Test Structure

Organize tests by functionality:

```
tests/
├── __init__.py
├── conftest.py              # Shared fixtures
├── test_server.py           # Server tests
├── test_tools.py            # Tool tests
├── test_resources.py        # Resource tests
├── test_auth.py             # Authentication tests
└── test_integration.py      # Integration tests
```

### Writing Tests

#### Unit Tests

```python
import pytest
from src.services.hello_world_service import HelloWorldService


def test_hello_world():
    """Test basic hello world functionality."""
    service = HelloWorldService()
    result = service.greet("World")
    assert result == "Hello, World!"


def test_hello_world_empty_name():
    """Test hello world with empty name."""
    service = HelloWorldService()
    with pytest.raises(ValueError, match="Name cannot be empty"):
        service.greet("")
```

#### Integration Tests

```python
import pytest
from fastmcp import FastMCP
from unittest.mock import Mock


@pytest.mark.asyncio
async def test_tool_integration():
    """Test tool integration."""
    mcp = FastMCP("TestServer")
    
    @mcp.tool()
    def add(a: float, b: float) -> float:
        return a + b
    
    # Verify tool is registered
    assert "add" in [tool.name for tool in mcp.list_tools()]
    
    # Test tool execution
    result = add(2, 3)
    assert result == 5
```

#### Fixtures

```python
# conftest.py
import pytest
from src.config import Settings


@pytest.fixture
def test_settings():
    """Provide test settings."""
    return Settings(
        server_name="test-server",
        auth_enabled=False,
        telemetry_enabled=False,
    )


@pytest.fixture
def mock_context():
    """Provide mock context."""
    from unittest.mock import Mock
    from fastmcp import Context
    
    ctx = Mock(spec=Context)
    return ctx
```

### Test Coverage

Maintain **>80%** test coverage:

```bash
# Generate coverage report
pytest --cov=src --cov-report=html

# View report
open htmlcov/index.html
```

## Documentation

### Docstring Format

Use **Google-style** docstrings:

```python
def process_data(data: str, format: str = "json") -> dict:
    """Process input data and return structured output.
    
    This function takes raw data and converts it to the specified format,
    performing validation and transformation as needed.
    
    Args:
        data: Raw input data to process
        format: Output format (json, xml, yaml). Defaults to "json"
        
    Returns:
        Dictionary containing processed data with metadata
        
    Raises:
        ValueError: If data is empty or format is unsupported
        ParseError: If data cannot be parsed
        
    Examples:
        >>> process_data('{"key": "value"}', format="json")
        {"data": {"key": "value"}, "format": "json"}
    """
    pass
```

### Update Documentation

When adding features:

1. **Update relevant docs** in `docs/` directory
2. **Add examples** to `docs/examples/README.md`
3. **Update API reference** in `docs/api-reference/README.md`
4. **Add usage guide** to appropriate guide in `docs/guides/`

### Documentation Build

Check documentation locally:

```bash
# Preview markdown files
# Use VS Code or any markdown viewer

# Check internal links
# (Add link checker if needed)
```

## Submitting Changes

### Branch Naming

Use descriptive branch names:

- `feature/add-oauth-support` - New features
- `fix/authentication-bug` - Bug fixes
- `docs/update-deployment-guide` - Documentation updates
- `refactor/simplify-telemetry` - Code refactoring
- `test/add-resource-tests` - Test additions

### Commit Messages

Follow conventional commits format:

```
type(scope): brief description

Detailed explanation of changes if needed.

Fixes #123
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code formatting
- `refactor`: Code refactoring
- `test`: Test changes
- `chore`: Build/config changes

**Examples:**

```bash
feat(tools): add data validation tool

Add new tool for validating user input data with Pydantic models.
Includes comprehensive error messages and type checking.

Fixes #42
```

```bash
fix(auth): resolve JWT token expiration issue

Token expiration was not being checked correctly. Now validates
expiration time before processing requests.

Fixes #56
```

### Pull Request Process

1. **Create feature branch** from `main`:

```bash
git checkout -b feature/my-new-feature
```

2. **Make changes** and commit:

```bash
git add .
git commit -m "feat(tools): add new feature"
```

3. **Push to your fork**:

```bash
git push origin feature/my-new-feature
```

4. **Create Pull Request** on GitHub:
   - Clear title and description
   - Link related issues
   - Add screenshots if UI changes
   - Request review from maintainers

5. **Address review feedback**:

```bash
# Make changes
git add .
git commit -m "fix: address review feedback"
git push origin feature/my-new-feature
```

6. **Squash commits** before merge (if requested):

```bash
git rebase -i HEAD~3  # Squash last 3 commits
git push --force-with-lease origin feature/my-new-feature
```

### Pull Request Checklist

Before submitting:

- [ ] Tests pass locally (`pytest`)
- [ ] Code is formatted (`black`, `isort`)
- [ ] Type checking passes (`pyright`)
- [ ] Documentation is updated
- [ ] Commit messages follow conventions
- [ ] Branch is up to date with `main`
- [ ] No merge conflicts
- [ ] PR description is clear and complete

## Release Process

### Version Numbers

Follow **Semantic Versioning**:

- `MAJOR.MINOR.PATCH` (e.g., `1.2.3`)
- `MAJOR`: Breaking changes
- `MINOR`: New features (backward compatible)
- `PATCH`: Bug fixes

### Creating a Release

1. **Update version** in `pyproject.toml`:

```toml
[project]
version = "1.2.0"
```

2. **Update CHANGELOG.md**:

```markdown
## [1.2.0] - 2025-01-15

### Added
- New OAuth2 authentication support
- Data validation tool with Pydantic

### Fixed
- JWT token expiration check
- Resource path parsing issue

### Changed
- Improved error messages for auth failures
```

3. **Create release commit**:

```bash
git add pyproject.toml CHANGELOG.md
git commit -m "chore: release v1.2.0"
git tag -a v1.2.0 -m "Release v1.2.0"
git push origin main --tags
```

4. **Create GitHub release**:
   - Go to GitHub releases
   - Create new release from tag
   - Add release notes from CHANGELOG
   - Upload any artifacts if needed

## Community Guidelines

### Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Help others learn and grow
- Follow project guidelines

### Getting Help

- **Issues**: Open GitHub issue for bugs or features
- **Discussions**: Use GitHub Discussions for questions
- **Documentation**: Check docs/ directory first
- **Examples**: See docs/examples/ for code samples

### Recognition

Contributors are recognized in:
- GitHub contributors list
- CHANGELOG.md for significant contributions
- Special thanks in release notes

## Additional Resources

- [FastMCP Documentation](https://github.com/jlowin/fastmcp)
- [MCP Protocol Specification](https://spec.modelcontextprotocol.io)
- [Python Packaging Guide](https://packaging.python.org)
- [Semantic Versioning](https://semver.org)
- [Conventional Commits](https://www.conventionalcommits.org)

## Questions?

If you have questions about contributing:

1. Check existing documentation
2. Search closed issues and PRs
3. Open a GitHub Discussion
4. Contact maintainers

Thank you for contributing! 🎉
