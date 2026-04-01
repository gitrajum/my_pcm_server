"""
Unit tests for hello_world_service.
"""

import pytest


@pytest.mark.unit
@pytest.mark.asyncio
async def test_hello_world():
    """Test the hello_world tool."""
    from src.services.hello_world_service import register_hello_world_tools
    from fastmcp import FastMCP

    mcp = FastMCP(name="test")
    register_hello_world_tools(mcp)

    # Test default greeting
    result = await mcp.call_tool("hello_world", {})
    assert "Hello, World!" in result
    assert "ElsaMcp" in result

    # Test custom name
    result = await mcp.call_tool("hello_world", {"name": "Alice"})
    assert "Hello, Alice!" in result


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo():
    """Test the echo tool."""
    from src.services.hello_world_service import register_hello_world_tools
    from fastmcp import FastMCP

    mcp = FastMCP(name="test")
    register_hello_world_tools(mcp)

    test_message = "test message 123"
    result = await mcp.call_tool("echo", {"message": test_message})
    assert result == test_message


@pytest.mark.unit
@pytest.mark.asyncio
async def test_add_numbers():
    """Test the add_numbers tool."""
    from src.services.hello_world_service import register_hello_world_tools
    from fastmcp import FastMCP

    mcp = FastMCP(name="test")
    register_hello_world_tools(mcp)

    # Test integer addition
    result = await mcp.call_tool("add_numbers", {"a": 5, "b": 3})
    assert result == 8

    # Test float addition
    result = await mcp.call_tool("add_numbers", {"a": 2.5, "b": 3.7})
    assert abs(result - 6.2) < 0.001


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reverse_string():
    """Test the reverse_string tool."""
    from src.services.hello_world_service import register_hello_world_tools
    from fastmcp import FastMCP

    mcp = FastMCP(name="test")
    register_hello_world_tools(mcp)

    result = await mcp.call_tool("reverse_string", {"text": "hello"})
    assert result == "olleh"

    result = await mcp.call_tool("reverse_string", {"text": "12345"})
    assert result == "54321"
