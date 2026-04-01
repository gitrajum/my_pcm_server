"""
Docker build and push commands.
"""

import subprocess
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

console = Console()
app = typer.Typer(help="Docker image management")


@app.command()
def build(
    tag: str = typer.Option(
        "latest", "--tag", "-t", help="Image tag"
    ),
    registry: Optional[str] = typer.Option(
        None, "--registry", "-r", help="Container registry URL"
    ),
    platform: str = typer.Option(
        "linux/amd64", "--platform", help="Target platform"
    ),
):
    """Build Docker image for MCP server."""
    project_root = Path(__file__).parent.parent.parent
    
    image_name = "grafana-mcp-server"
    if registry:
        full_image = f"{registry}/{image_name}:{tag}"
    else:
        full_image = f"{image_name}:{tag}"
    
    console.print(f"[cyan]→[/cyan] Building Docker image: {full_image}")
    
    cmd = [
        "docker", "build",
        "--platform", platform,
        "-t", full_image,
        "-f", "Dockerfile",
        ".",
    ]
    
    result = subprocess.run(cmd, cwd=project_root)
    
    if result.returncode == 0:
        console.print(f"[green]✓[/green] Image built: {full_image}")
    else:
        console.print("[red]✗[/red] Build failed")
        raise typer.Exit(1)


@app.command()
def push(
    tag: str = typer.Option(
        "latest", "--tag", "-t", help="Image tag"
    ),
    registry: str = typer.Option(
        ..., "--registry", "-r", help="Container registry URL"
    ),
):
    """Push Docker image to container registry."""
    image_name = "grafana-mcp-server"
    full_image = f"{registry}/{image_name}:{tag}"
    
    console.print(f"[cyan]→[/cyan] Pushing image: {full_image}")
    
    cmd = ["docker", "push", full_image]
    
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        console.print(f"[green]✓[/green] Image pushed: {full_image}")
    else:
        console.print("[red]✗[/red] Push failed")
        raise typer.Exit(1)


@app.command()
def login(
    registry: str = typer.Option(
        ..., "--registry", "-r", help="Container registry URL"
    ),
):
    """Login to Azure Container Registry."""
    console.print(f"[cyan]→[/cyan] Logging in to: {registry}")
    
    # Extract registry name from URL
    registry_name = registry.replace(".azurecr.io", "").replace("https://", "")
    
    cmd = ["az", "acr", "login", "--name", registry_name]
    
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        console.print(f"[green]✓[/green] Logged in to {registry}")
    else:
        console.print("[red]✗[/red] Login failed")
        raise typer.Exit(1)
