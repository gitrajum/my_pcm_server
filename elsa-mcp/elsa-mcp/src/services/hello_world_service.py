"""
Hello World Service - Example MCP Tool
Demonstrates basic tool implementation pattern.
"""

import logging
from typing import Optional

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

logger = logging.getLogger(__name__)


def register_hello_world_tools(mcp: FastMCP):
    """Register hello world tools with the MCP server."""

    @mcp.tool()
    async def hello_world(name: str = "World") -> str:
        """
        A simple hello world tool.

        Args:
            name: Name to greet (default: "World")

        Returns:
            A greeting message
        """
        logger.info(f"hello_world called with name={name}")
        return f"Hello, {name}! Welcome to ElsaMcp."

    @mcp.tool()
    async def echo(message: str) -> str:
        """
        Echo back a message.

        Args:
            message: The message to echo

        Returns:
            The same message
        """
        logger.info(f"echo called with message={message}")
        return message

    @mcp.tool()
    async def add_numbers(a: float, b: float) -> float:
        """
        Add two numbers together.

        Args:
            a: First number
            b: Second number

        Returns:
            The sum of a and b
        """
        logger.info(f"add_numbers called with a={a}, b={b}")
        result = a + b
        return result

    @mcp.tool()
    async def reverse_string(text: str) -> str:
        """
        Reverse a string.

        Args:
            text: The text to reverse

        Returns:
            The reversed text
        """
        logger.info(f"reverse_string called with text={text}")
        return text[::-1]

    logger.info("Hello World tools registered")
