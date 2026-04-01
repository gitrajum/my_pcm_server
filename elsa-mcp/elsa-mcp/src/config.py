"""
Configuration management for ElsaMcp MCP Server.
"""

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Server Configuration
    mcp_server_host: str = Field(default="0.0.0.0", description="Server host address")
    mcp_server_port: int = Field(default=8000, description="Server port")

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format string",
    )

    # Authentication (Optional)
    mcp_auth_enabled: bool = Field(default=False, description="Enable authentication")
    mcp_auth_debug: bool = Field(default=True, description="Auth debug mode")
    auth_resource_server_url: Optional[str] = Field(
        default=None, description="OAuth2 resource server URL"
    )
    auth_client_id: Optional[str] = Field(default=None, description="OAuth2 client ID")
    auth_audience: Optional[str] = Field(default=None, description="OAuth2 audience")

    # Telemetry (Optional)
    otel_enabled: bool = Field(default=False, description="Enable OpenTelemetry")
    otel_service_name: str = Field(default="elsa-mcp", description="Service name for telemetry")
    otel_exporter_otlp_endpoint: str = Field(
        default="http://localhost:4318", description="OTLP endpoint"
    )

    # Databricks Connection (ELSA UC3)
    databricks_server_hostname: str = Field(
        default="adb-4071335540424391.11.azuredatabricks.net",
        description="Databricks server hostname",
    )
    databricks_http_path: str = Field(
        default="/sql/1.0/warehouses/916c447fdd11cd1e",
        description="SQL warehouse HTTP path",
    )
    databricks_access_token: str = Field(
        default="",
        description="Databricks personal access token",
    )
    databricks_catalog: str = Field(
        default="efdataonelh_prd",
        description="Default Unity Catalog catalog",
    )
    databricks_schema: str = Field(
        default="generaldiscovery_servicenow_r",
        description="Default schema",
    )
    databricks_use_proxy: bool = Field(
        default=False,
        description="Route Databricks traffic through a proxy",
    )
    databricks_proxy_host: Optional[str] = Field(
        default=None, description="Proxy hostname"
    )
    databricks_proxy_port: Optional[int] = Field(
        default=8080, description="Proxy port"
    )


@lru_cache
def get_settings() -> Settings:
    """Get cached application settings."""
    return Settings()
