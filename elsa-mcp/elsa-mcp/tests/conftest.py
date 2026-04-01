"""
Pytest configuration and fixtures.
"""

import os
import sys
from pathlib import Path

import pytest

# Add src to path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    """Setup test environment variables."""
    os.environ["MCP_AUTH_ENABLED"] = "false"
    os.environ["OTEL_ENABLED"] = "false"
    os.environ["LOG_LEVEL"] = "DEBUG"
    yield
    # Cleanup if needed
