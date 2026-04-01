"""
ElsaMcp CLI Tool

Manage infrastructure and deployment for ElsaMcp MCP server.

Usage:
    elsa-mcp-cli docker [COMMAND]  # Docker image management
    elsa-mcp-cli infra [COMMAND]   # Infrastructure operations
"""

import typer
from rich.console import Console

from .commands import iac, docker

console = Console()

app = typer.Typer(
    name="elsa-mcp-cli",
    help="CLI tool for ElsaMcp MCP server infrastructure and deployment",
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
    console.print(f"elsa-mcp-cli version {__version__}")


if __name__ == "__main__":
    app()
