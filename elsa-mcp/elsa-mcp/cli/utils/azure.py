"""Azure CLI utility functions."""

import json
import subprocess
import sys
from typing import Optional


def run_az_command(command: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run an Azure CLI command."""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=check
        )
        return result
    except subprocess.CalledProcessError as e:
        if check:
            raise
        return e


def ensure_azure_login() -> bool:
    """Ensure user is logged in to Azure CLI."""
    result = run_az_command(["az", "account", "show"], check=False)
    if result.returncode != 0:
        print("❌ Not logged in to Azure CLI. Please run: az login")
        sys.exit(1)
    return True


def check_resource_group(name: str, subscription_id: str) -> bool:
    """Check if resource group exists."""
    result = run_az_command([
        "az", "group", "exists",
        "--name", name,
        "--subscription", subscription_id
    ], check=False)
    return result.stdout.strip().lower() == "true"


def create_resource_group(name: str, location: str, subscription_id: str) -> bool:
    """Create resource group."""
    result = run_az_command([
        "az", "group", "create",
        "--name", name,
        "--location", location,
        "--subscription", subscription_id
    ])
    return result.returncode == 0


def check_storage_account(name: str, resource_group: str, subscription_id: str) -> bool:
    """Check if storage account exists."""
    result = run_az_command([
        "az", "storage", "account", "show",
        "--name", name,
        "--resource-group", resource_group,
        "--subscription", subscription_id
    ], check=False)
    return result.returncode == 0


def create_storage_account(name: str, resource_group: str, location: str, subscription_id: str) -> bool:
    """Create storage account."""
    result = run_az_command([
        "az", "storage", "account", "create",
        "--name", name,
        "--resource-group", resource_group,
        "--location", location,
        "--sku", "Standard_LRS",
        "--min-tls-version", "TLS1_2",
        "--allow-blob-public-access", "false",
        "--subscription", subscription_id
    ])
    return result.returncode == 0


def check_storage_container(name: str, storage_account: str, resource_group: str, subscription_id: str) -> bool:
    """Check if storage container exists."""
    result = run_az_command([
        "az", "storage", "container", "exists",
        "--name", name,
        "--account-name", storage_account,
        "--auth-mode", "login",
        "--subscription", subscription_id
    ], check=False)
    
    if result.returncode == 0:
        try:
            data = json.loads(result.stdout)
            return data.get("exists", False)
        except json.JSONDecodeError:
            return False
    return False


def create_storage_container(name: str, storage_account: str, resource_group: str, subscription_id: str) -> bool:
    """Create storage container."""
    result = run_az_command([
        "az", "storage", "container", "create",
        "--name", name,
        "--account-name", storage_account,
        "--auth-mode", "login",
        "--subscription", subscription_id
    ])
    return result.returncode == 0


def check_container_registry(name: str, subscription_id: str) -> bool:
    """Check if container registry exists."""
    result = run_az_command([
        "az", "acr", "show",
        "--name", name,
        "--subscription", subscription_id
    ], check=False)
    return result.returncode == 0


def create_container_registry(name: str, resource_group: str, location: str, subscription_id: str) -> bool:
    """Create container registry."""
    result = run_az_command([
        "az", "acr", "create",
        "--name", name,
        "--resource-group", resource_group,
        "--location", location,
        "--sku", "Basic",
        "--admin-enabled", "true",
        "--subscription", subscription_id
    ])
    return result.returncode == 0
