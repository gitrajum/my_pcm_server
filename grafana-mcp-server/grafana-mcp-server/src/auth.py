"""
Custom authentication for FastMCP using AgenticAI SDK.

This module provides FastMCP-compatible token verification using the AgenticAI SDK's
EntraIDValidator for Azure AD JWT token validation.
"""

import logging
from typing import Optional

from fastmcp.server.auth import AccessToken, TokenVerifier

from agenticai.auth import EntraIDValidator
from agenticai.config import get_config

logger = logging.getLogger(__name__)


class EntraIDTokenVerifier(TokenVerifier):
    """
    FastMCP TokenVerifier that uses AgenticAI SDK's EntraIDValidator.

    This class bridges FastMCP's authentication system with the AgenticAI SDK's
    Azure AD authentication, enabling MCP servers to validate JWT tokens from:
    - Managed Identity (for agent-to-MCP communication)
    - User tokens (for passthrough authentication)

    Example:
        ```python
        # In src/server.py
        from src.auth import EntraIDTokenVerifier

        auth = EntraIDTokenVerifier()
        mcp = FastMCP("My MCP Server", auth=auth)
        ```

    Architecture:
        FastMCP → EntraIDTokenVerifier → SDK EntraIDValidator → Azure AD JWKS

    Bayer Compliance:
        - 4.2.18-4.2.25: TLS 1.2+ via Azure AD
        - 4.2.36-4.2.40: Security logging (all auth events, no secrets)
    """

    def __init__(
        self,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        audience: Optional[str] = None,
        issuer: Optional[str] = None,
    ):
        """
        Initialize EntraID token verifier with Azure AD configuration.

        Args:
            tenant_id: Azure AD tenant ID (defaults to config)
            client_id: App registration client ID (defaults to config)
            audience: Expected token audience (defaults to api://{client_id})
            issuer: Expected token issuer (defaults to Azure AD tenant issuer)
        """
        super().__init__(
            base_url=None,  # Not serving OAuth endpoints
            required_scopes=None,  # Scope validation in EntraIDValidator
        )

        # Load configuration
        config = get_config()

        # Use provided values or fall back to config
        self.tenant_id = tenant_id or config.azure_tenant_id
        self.client_id = client_id or config.managed_identity_client_id
        self.audience = audience or f"api://{self.client_id}"
        self.issuer = issuer or f"https://login.microsoftonline.com/{self.tenant_id}/v2.0"

        # Create SDK validator
        self.validator = EntraIDValidator(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            audience=self.audience,
            issuer=self.issuer,
        )

        logger.info("EntraIDTokenVerifier initialized")
        logger.info(f"  Tenant ID: {self.tenant_id}")
        logger.info(f"  Client ID: {self.client_id}")
        logger.info(f"  Audience: {self.audience}")

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        """
        Verify JWT token using SDK's EntraIDValidator.

        Args:
            token: JWT token string (Bearer token without "Bearer " prefix)

        Returns:
            AccessToken with user info and scopes, or None if validation fails

        Note:
            Logs authentication events per Bayer requirement 4.2.36
        """
        try:
            # Validate token using SDK
            claims = self.validator.validate_token(token)

            # Extract user identity
            user = self.validator.extract_user_identity(claims)

            # Extract scopes
            scopes_str = claims.get("scp", "") or claims.get("roles", "")
            scopes = scopes_str.split() if scopes_str else []

            # Log authentication event (Bayer 4.2.36)
            logger.info(
                "MCP tool access authenticated",
                extra={
                    "event": "mcp_authentication_success",
                    "user_id": user.get("user_id"),
                    "user_email": user.get("email"),
                    "tenant_id": user.get("tenant_id"),
                    "scopes": scopes,
                    "token_type": "managed_identity" if "idtyp" in claims else "user",
                },
            )

            # Convert to FastMCP AccessToken
            return AccessToken(
                token=token,
                scopes=scopes,
                expires_at=claims.get("exp"),
                # Include user info for tool access
                user_id=user.get("user_id"),
                email=user.get("email"),
                name=user.get("name"),
                tenant_id=user.get("tenant_id"),
            )

        except ValueError as e:
            # Token validation failed
            logger.warning(
                f"Token validation failed: {e}",
                extra={
                    "event": "mcp_authentication_failed",
                    "error": str(e),
                },
            )
            return None
        except Exception as e:
            # Unexpected error
            logger.error(
                f"Unexpected error during token validation: {e}",
                extra={
                    "event": "mcp_authentication_error",
                    "error": str(e),
                },
            )
            return None
