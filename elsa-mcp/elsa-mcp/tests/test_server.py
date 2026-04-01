"""
Integration tests for the MCP server.
"""

import pytest


@pytest.mark.integration
def test_server_config():
    """Test server configuration loading."""
    from src.config import get_settings

    settings = get_settings()
    assert settings.mcp_server_port == 8000
    assert settings.log_level in ["DEBUG", "INFO", "WARNING", "ERROR"]


@pytest.mark.integration
def test_server_initialization():
    """Test MCP server can be initialized."""
    from src.main import mcp

    assert mcp.name == "elsa-mcp"
    assert mcp.version == "0.1.0"
    # instructions is set but not directly accessible as an attribute


@pytest.mark.integration
def test_resources_available():
    """Test that resources are registered."""
    from src.main import mcp

    # Check config resource is available
    resources = mcp.list_resources()
    assert any(r.uri == "config://settings" for r in resources)


@pytest.mark.integration
def test_prompts_available():
    """Test that prompts are registered."""
    from src.main import mcp

    # Check welcome prompt is available
    prompts = mcp.list_prompts()
    assert any(p.name == "welcome" for p in prompts)
