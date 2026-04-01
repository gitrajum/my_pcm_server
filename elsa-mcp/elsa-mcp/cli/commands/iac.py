"""
Infrastructure as Code (Terraform) commands.
"""

import subprocess
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from ..utils import azure

console = Console()
app = typer.Typer(help="Terraform infrastructure management")


def _get_terraform_dir(terraform_dir: Optional[Path], environment: str) -> Path:
    """Resolve terraform directory based on environment."""
    valid_environments = ["test", "staging", "prod"]
    
    if environment not in valid_environments:
        console.print(f"[red]✗[/red] Invalid environment: {environment}")
        console.print(f"[dim]Valid environments: {', '.join(valid_environments)}[/dim]")
        raise typer.Exit(1)
    
    if terraform_dir is None:
        project_root = Path.cwd()
        terraform_base = project_root / "terraform"
        terraform_dir = terraform_base / "environments" / environment
        
        if not terraform_dir.exists():
            console.print(f"[red]✗[/red] Terraform directory not found: {terraform_dir}")
            raise typer.Exit(1)
    else:
        if terraform_dir.name not in valid_environments:
            terraform_dir = terraform_dir / "environments" / environment
            if not terraform_dir.exists():
                console.print(f"[red]✗[/red] Terraform directory not found: {terraform_dir}")
                raise typer.Exit(1)
    
    return terraform_dir


@app.command()
def init(
    environment: str = typer.Option("test", "--env", "-e", help="Environment (test, staging, prod)"),
    subscription_id: str = typer.Option(..., "--subscription-id", "-s", help="Azure subscription ID"),
    state_rg: str = typer.Option(..., "--state-rg", help="Resource group for Terraform state"),
    state_storage: str = typer.Option(..., "--state-storage", help="Storage account for Terraform state"),
    state_container: str = typer.Option("tfstate", "--state-container", help="Container for Terraform state"),
    terraform_dir: Optional[Path] = typer.Option(None, "--dir", "-d", help="Path to Terraform directory"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show commands without executing"),
):
    """Initialize Terraform backend and download providers.
    
    Examples:
        # Initialize with remote state for test environment
        elsa-mcp-cli infra init --env test -s <sub-id> --state-rg state-rg --state-storage mystatestorage
    """
    terraform_dir = _get_terraform_dir(terraform_dir, environment)
    if not terraform_dir.exists():
        console.print("[red]✗[/red] Terraform directory not found")
        raise typer.Exit(1)
    
    console.print(f"\n[cyan]→[/cyan] Initializing Terraform for [bold]{environment}[/bold] environment...")
    
    init_cmd = [
        "terraform", "init",
        f"-backend-config=resource_group_name={state_rg}",
        f"-backend-config=storage_account_name={state_storage}",
        f"-backend-config=container_name={state_container}",
        f"-backend-config=key=elsa-mcp-{environment}.tfstate",
    ]
    
    if dry_run:
        console.print(f"[dim]$ {' '.join(init_cmd)}[/dim]")
    else:
        result = subprocess.run(init_cmd, cwd=terraform_dir, capture_output=True, text=True)
        if result.returncode != 0:
            console.print(f"[red]✗[/red] Terraform init failed:\n{result.stderr}")
            raise typer.Exit(1)
    
    console.print("[green]✓[/green] Terraform initialized")


@app.command()
def plan(
    environment: str = typer.Option("test", "--env", "-e", help="Environment (test, staging, prod)"),
    container_image: str = typer.Option(..., "--container-image", "-i", help="Container image for MCP server"),
    state_rg: str = typer.Option(..., "--state-rg", help="Resource group for Terraform state"),
    state_storage: str = typer.Option(..., "--state-storage", help="Storage account for Terraform state"),
    terraform_dir: Optional[Path] = typer.Option(None, "--dir", "-d", help="Path to Terraform directory"),
    destroy: bool = typer.Option(False, "--destroy", help="Plan for destroy"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show commands without executing"),
):
    """Generate and show Terraform execution plan.
    
    Examples:
        # Plan deployment for test environment
        elsa-mcp-cli infra plan --env test -i myacr.azurecr.io/mcp:latest --state-rg state-rg --state-storage mystatestorage
        
        # Plan destruction
        elsa-mcp-cli infra plan --env prod -i myacr.azurecr.io/mcp:latest --state-rg state-rg --state-storage mystatestorage --destroy
    """
    terraform_dir = _get_terraform_dir(terraform_dir, environment)
    if not terraform_dir.exists():
        console.print("[red]✗[/red] Terraform directory not found")
        raise typer.Exit(1)
    
    # Ensure initialized
    init_cmd = [
        "terraform", "init",
        f"-backend-config=resource_group_name={state_rg}",
        f"-backend-config=storage_account_name={state_storage}",
        f"-backend-config=container_name=tfstate",
        f"-backend-config=key=elsa-mcp-{environment}.tfstate",
    ]
    
    if not dry_run:
        subprocess.run(init_cmd, cwd=terraform_dir, capture_output=True, check=True)
    
    # Validate
    console.print(f"\n[cyan]→[/cyan] Validating Terraform configuration...")
    if not dry_run:
        result = subprocess.run(["terraform", "validate"], cwd=terraform_dir, capture_output=True, text=True)
        if result.returncode != 0:
            console.print(f"[red]✗[/red] Terraform validation failed:\n{result.stderr}")
            raise typer.Exit(1)
    console.print("[green]✓[/green] Configuration valid")
    
    # Plan
    console.print(f"\n[cyan]→[/cyan] Planning infrastructure changes for [bold]{environment}[/bold]...")
    plan_cmd = [
        "terraform", "plan",
        f"-var=container_image={container_image}",
    ]
    
    if destroy:
        plan_cmd.append("-destroy")
    else:
        plan_cmd.extend(["-out=tfplan"])
    
    if dry_run:
        console.print(f"[dim]$ {' '.join(plan_cmd)}[/dim]")
    else:
        result = subprocess.run(plan_cmd, cwd=terraform_dir)
        if result.returncode != 0:
            console.print(f"[red]✗[/red] Terraform plan failed")
            raise typer.Exit(1)
    
    console.print("[green]✓[/green] Plan generated")


@app.command()
def apply(
    environment: str = typer.Option("test", "--env", "-e", help="Environment (test, staging, prod)"),
    container_image: str = typer.Option(..., "--container-image", "-i", help="Container image for MCP server"),
    state_rg: str = typer.Option(..., "--state-rg", help="Resource group for Terraform state"),
    state_storage: str = typer.Option(..., "--state-storage", help="Storage account for Terraform state"),
    terraform_dir: Optional[Path] = typer.Option(None, "--dir", "-d", help="Path to Terraform directory"),
    auto_approve: bool = typer.Option(False, "--auto-approve", "-y", help="Auto-approve Terraform changes"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show commands without executing"),
):
    """Apply Terraform changes to deploy MCP server infrastructure.
    
    This command:
    1. Checks Azure CLI login
    2. Initializes Terraform
    3. Plans and applies infrastructure changes
    
    Examples:
        # Deploy MCP server to test environment
        elsa-mcp-cli infra apply --env test -i myacr.azurecr.io/mcp:latest --state-rg state-rg --state-storage mystatestorage
        
        # Deploy to prod without approval (CI/CD)
        elsa-mcp-cli infra apply --env prod -i myacr.azurecr.io/mcp:latest --state-rg state-rg --state-storage mystatestorage --auto-approve
    """
    console.print(Panel.fit(
        f"[bold cyan]ElsaMcp Infrastructure Deployment[/bold cyan]\n[dim]Environment: {environment}[/dim]",
        border_style="cyan"
    ))
    
    # Get terraform directory
    terraform_dir = _get_terraform_dir(terraform_dir, environment)
    if not terraform_dir.exists():
        console.print("[red]✗[/red] Terraform directory not found")
        raise typer.Exit(1)
    
    try:
        # 1. Ensure Azure CLI login
        console.print("\n[cyan]→[/cyan] Checking Azure CLI login...")
        if not dry_run:
            azure.ensure_azure_login()
        console.print("[green]✓[/green] Azure CLI authenticated")
        
        # 2. Ensure shared infrastructure exists
        if ensure_shared_infra:
            console.print(f"\n[cyan]→[/cyan] Ensuring shared infrastructure...")
            
            # Check/create state resource group
            if not dry_run and not azure.check_resource_group(state_rg, subscription_id):
                console.print(f"[yellow]![/yellow] Creating state resource group: {state_rg}")
                azure.create_resource_group(state_rg, location, subscription_id)
            console.print(f"[green]✓[/green] State resource group: {state_rg}")
            
            # Check/create state storage account
            if not dry_run and not azure.check_storage_account(state_storage, state_rg, subscription_id):
                console.print(f"[yellow]![/yellow] Creating state storage account: {state_storage}")
                azure.create_storage_account(state_storage, state_rg, location, subscription_id)
            console.print(f"[green]✓[/green] State storage account: {state_storage}")
            
            # Check/create tfstate container
            if not dry_run and not azure.check_storage_container("tfstate", state_storage, state_rg, subscription_id):
                console.print(f"[yellow]![/yellow] Creating tfstate container")
                azure.create_storage_container("tfstate", state_storage, state_rg, subscription_id)
            console.print(f"[green]✓[/green] Tfstate container ready")
        
        # 3. Terraform init
        console.print(f"\n[cyan]→[/cyan] Initializing Terraform...")
        init_cmd = [
            "terraform", "init",
            f"-backend-config=resource_group_name={state_rg}",
            f"-backend-config=storage_account_name={state_storage}",
            f"-backend-config=container_name=tfstate",
            f"-backend-config=key=elsa-mcp.tfstate",
        ]
        
        if dry_run:
            console.print(f"[dim]$ {' '.join(init_cmd)}[/dim]")
        else:
            result = subprocess.run(init_cmd, cwd=terraform_dir, capture_output=True, text=True)
            if result.returncode != 0:
                console.print(f"[red]✗[/red] Terraform init failed:\n{result.stderr}")
                raise typer.Exit(1)
        console.print("[green]✓[/green] Terraform initialized")
        
        # 4. Terraform validate
        console.print(f"\n[cyan]→[/cyan] Validating Terraform configuration...")
        validate_cmd = ["terraform", "validate"]
        
        if dry_run:
            console.print(f"[dim]$ {' '.join(validate_cmd)}[/dim]")
        else:
            result = subprocess.run(validate_cmd, cwd=terraform_dir, capture_output=True, text=True)
            if result.returncode != 0:
                console.print(f"[red]✗[/red] Terraform validation failed:\n{result.stderr}")
                raise typer.Exit(1)
        console.print("[green]✓[/green] Configuration valid")
        
        # 5. Terraform plan
        console.print(f"\n[cyan]→[/cyan] Planning infrastructure changes...")
        plan_cmd = [
            "terraform", "plan",
            f"-var=subscription_id={subscription_id}",
            f"-var=resource_group_name={resource_group}",
            f"-var=location={location}",
            f"-var=state_resource_group_name={state_rg}",
            f"-var=state_storage_account_name={state_storage}",
            f"-var=mcp_container_image={container_image}",
            "-out=tfplan",
        ]
        
        if dry_run:
            console.print(f"[dim]$ {' '.join(plan_cmd)}[/dim]")
        else:
            result = subprocess.run(plan_cmd, cwd=terraform_dir)
            if result.returncode != 0:
                console.print(f"[red]✗[/red] Terraform plan failed")
                raise typer.Exit(1)
        console.print("[green]✓[/green] Plan created")
        
        # 6. Terraform apply
        if not auto_approve and not dry_run:
            proceed = typer.confirm("\nProceed with deployment?")
            if not proceed:
                console.print("[yellow]Deployment cancelled[/yellow]")
                raise typer.Exit(0)
        
        console.print(f"\n[cyan]→[/cyan] Applying infrastructure changes...")
        apply_cmd = ["terraform", "apply"]
        if auto_approve:
            apply_cmd.append("-auto-approve")
        apply_cmd.append("tfplan")
        
        if dry_run:
            console.print(f"[dim]$ {' '.join(apply_cmd)}[/dim]")
        else:
            result = subprocess.run(apply_cmd, cwd=terraform_dir)
            if result.returncode != 0:
                console.print(f"[red]✗[/red] Terraform apply failed")
                raise typer.Exit(1)
        
        console.print("\n[green]✓[/green] [bold]Infrastructure deployed successfully![/bold]")
        
        # Show outputs
        if not dry_run:
            console.print(f"\n[cyan]→[/cyan] Infrastructure outputs:")
            subprocess.run(["terraform", "output"], cwd=terraform_dir)
        
    except Exception as e:
        console.print(f"\n[red]✗[/red] Deployment failed: {e}")
        raise typer.Exit(1)


# Keep legacy deploy command for backward compatibility
@app.command(hidden=True)
def deploy(
    subscription_id: str = typer.Option(..., "--subscription-id", "-s"),
    resource_group: str = typer.Option("rg-elsa-mcp-dev", "--resource-group", "-g"),
    location: str = typer.Option("eastus", "--location", "-l"),
    state_rg: str = typer.Option(..., "--state-rg"),
    state_storage: str = typer.Option(..., "--state-storage"),
    container_image: str = typer.Option(..., "--container-image", "-i"),
    auto_approve: bool = typer.Option(False, "--auto-approve", "-y"),
    ensure_shared_infra: bool = typer.Option(True, "--ensure-shared-infra/--skip-shared-infra"),
    dry_run: bool = typer.Option(False, "--dry-run"),
):
    """[DEPRECATED] Use 'infra apply' instead."""
    console.print("[yellow]⚠[/yellow]  The 'deploy' command is deprecated. Use 'infra apply' instead.")
    # Redirect to apply
    apply(
        subscription_id=subscription_id,
        resource_group=resource_group,
        location=location,
        state_rg=state_rg,
        state_storage=state_storage,
        container_image=container_image,
        auto_approve=auto_approve,
        ensure_shared_infra=ensure_shared_infra,
        dry_run=dry_run,
    )


@app.command()
def destroy(
    subscription_id: str = typer.Option(..., "--subscription-id", "-s", help="Azure subscription ID"),
    resource_group: str = typer.Option("rg-elsa-mcp-dev", "--resource-group", "-g", help="Resource group name"),
    location: str = typer.Option("eastus", "--location", "-l", help="Azure region"),
    state_rg: str = typer.Option(..., "--state-rg", help="Resource group for Terraform state"),
    state_storage: str = typer.Option(..., "--state-storage", help="Storage account for Terraform state"),
    container_image: str = typer.Option(..., "--container-image", "-i", help="Container image for MCP server"),
    auto_approve: bool = typer.Option(False, "--auto-approve", "-y", help="Auto-approve destruction"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show commands without executing"),
):
    """Destroy MCP server infrastructure.
    
    Examples:
        # Destroy infrastructure
        elsa-mcp-cli infra destroy -s <sub-id> --state-rg state-rg --state-storage mystatestorage -i myacr.azurecr.io/mcp:latest
        
        # Destroy without approval
        elsa-mcp-cli infra destroy -s <sub-id> --state-rg state-rg --state-storage mystatestorage -i myacr.azurecr.io/mcp:latest --auto-approve
    """
    if not auto_approve:
        console.print("[yellow]⚠[/yellow]  [bold]WARNING: This will destroy all infrastructure![/bold]")
        proceed = typer.confirm("Are you sure you want to continue?")
        if not proceed:
            console.print("Cancelled")
            raise typer.Exit(0)
    
    terraform_dir = Path(__file__).parent.parent.parent / "terraform"
    if not terraform_dir.exists():
        console.print("[red]✗[/red] Terraform directory not found")
        raise typer.Exit(1)
    
    console.print(Panel.fit(
        f"[bold red]⚠️  DESTROY Infrastructure[/bold red]\n"
        f"Resource Group: [magenta]{resource_group}[/magenta]\n"
        f"This will delete all MCP server resources!",
        border_style="red"
    ))
    
    # Init first
    init_cmd = [
        "terraform", "init",
        f"-backend-config=resource_group_name={state_rg}",
        f"-backend-config=storage_account_name={state_storage}",
        f"-backend-config=container_name=tfstate",
        f"-backend-config=key=elsa-mcp.tfstate",
    ]
    
    if not dry_run:
        subprocess.run(init_cmd, cwd=terraform_dir, capture_output=True, check=True)
    
    # Destroy
    destroy_cmd = [
        "terraform", "destroy",
        f"-var=subscription_id={subscription_id}",
        f"-var=resource_group_name={resource_group}",
        f"-var=location={location}",
        f"-var=state_resource_group_name={state_rg}",
        f"-var=state_storage_account_name={state_storage}",
        f"-var=mcp_container_image={container_image}",
    ]
    
    if auto_approve:
        destroy_cmd.append("-auto-approve")
    
    if dry_run:
        console.print(f"[dim]$ {' '.join(destroy_cmd)}[/dim]")
    else:
        result = subprocess.run(destroy_cmd, cwd=terraform_dir)
        if result.returncode == 0:
            console.print("[green]✓[/green] Infrastructure destroyed")
        else:
            console.print("[red]✗[/red] Destroy failed")
            raise typer.Exit(1)


@app.command()def validate():
    """Validate Terraform configuration without accessing remote state.
    
    This runs 'terraform validate' to check syntax and configuration.
    Unlike 'plan', it doesn't access remote state or check actual resources.
    
    Examples:
        # Validate configuration
        elsa-mcp-cli infra validate
    """
    terraform_dir = Path(__file__).parent.parent.parent / "terraform"
    
    console.print("[cyan]Validating Terraform configuration...[/cyan]")
    
    cmd = ["terraform", "validate"]
    result = subprocess.run(cmd, cwd=terraform_dir, capture_output=True, text=True)
    
    if result.returncode == 0:
        console.print("[green]\u2713[/green] Configuration is valid")
        if result.stdout:
            console.print(result.stdout)
    else:
        console.print("[red]\u2717[/red] Configuration validation failed")
        if result.stderr:
            console.print(result.stderr)
        raise typer.Exit(1)


@app.command()def output(
    output_name: Optional[str] = typer.Argument(None, help="Specific output to show"),
    json: bool = typer.Option(False, "--json", help="Output in JSON format"),
):
    """Show Terraform outputs from deployed infrastructure.
    
    Examples:
        # Show all outputs
        elsa-mcp-cli infra output
        
        # Show specific output
        elsa-mcp-cli infra output container_app_url
        
        # JSON format
        elsa-mcp-cli infra output --json
    """
    terraform_dir = Path(__file__).parent.parent.parent / "terraform"
    
    cmd = ["terraform", "output"]
    
    if json:
        cmd.append("-json")
    
    if output_name:
        cmd.append(output_name)
    
    result = subprocess.run(cmd, cwd=terraform_dir)
    
    if result.returncode != 0:
        console.print("[red]✗[/red] Failed to retrieve outputs")
        raise typer.Exit(1)
