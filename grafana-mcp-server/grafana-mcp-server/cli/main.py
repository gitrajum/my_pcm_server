"""
GrafanaMcpServer CLI Tool

Manage infrastructure and deployment for GrafanaMcpServer MCP server.

Usage:
    grafana-mcp-server-cli docker [COMMAND]  # Docker image management
    grafana-mcp-server-cli infra [COMMAND]   # Infrastructure operations
"""

import typer
from rich.console import Console

from .commands import iac, docker

console = Console()

app = typer.Typer(
    name="grafana-mcp-server-cli",
    help="CLI tool for GrafanaMcpServer MCP server infrastructure and deployment",
    add_completion=False,
)

# Register commands
app.add_typer(docker.app, name="docker", help="🐳 Docker image management")
app.add_typer(iac.app, name="infra", help="🏗️  Infrastructure operations (Terraform)")

# Keep legacy name for backward compatibility
app.add_typer(iac.app, name="iac", help="[DEPRECATED] Use 'infra' instead", hidden=True)


@app.command()
def version():
    """Show CLI version."""
    from . import __version__
    console.print(f"grafana-mcp-server-cli version {__version__}")


if __name__ == "__main__":
    app()
