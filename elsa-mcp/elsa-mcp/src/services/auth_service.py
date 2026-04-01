"""
Authentication Service (Optional)
Provides JWT-based authentication for the MCP server.
"""

import logging
from typing import Optional

try:
    import jwt
    from jwt import PyJWKClient
except ImportError:
    raise ImportError(
        "Authentication requires 'pyjwt' and 'cryptography'. "
        "Install with: pip install elsa-mcp[auth]"
    )

from fastmcp import FastMCP

logger = logging.getLogger(__name__)


class JWTAuthenticator:
    """JWT token authenticator for OAuth2/Azure AD."""

    def __init__(
        self,
        resource_server_url: str,
        client_id: str,
        audience: str,
        debug: bool = False,
    ):
        """
        Initialize JWT authenticator.

        Args:
            resource_server_url: The OAuth2 resource server URL
            client_id: The OAuth2 client ID
            audience: The expected audience in tokens
            debug: Enable debug mode for authentication
        """
        self.resource_server_url = resource_server_url
        self.client_id = client_id
        self.audience = audience
        self.debug = debug

        # Initialize JWKS client
        jwks_uri = f"{resource_server_url.rstrip('/')}/discovery/v2.0/keys"
        self.jwks_client = PyJWKClient(jwks_uri)

        logger.info(f"JWT Authenticator initialized with JWKS URI: {jwks_uri}")

    def verify_token(self, token: str) -> Optional[dict]:
        """
        Verify a JWT token.

        Args:
            token: The JWT token to verify

        Returns:
            Decoded token claims if valid, None otherwise
        """
        try:
            # Get signing key from JWKS
            signing_key = self.jwks_client.get_signing_key_from_jwt(token)

            # Decode and verify token
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                audience=self.audience,
                options={"verify_exp": True},
            )

            logger.debug(f"Token verified successfully: {claims.get('sub')}")
            return claims

        except jwt.ExpiredSignatureError:
            logger.warning("Token has expired")
            return None
        except jwt.InvalidAudienceError:
            logger.warning(f"Invalid audience in token (expected: {self.audience})")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {e}")
            return None
        except Exception as e:
            logger.error(f"Token verification error: {e}", exc_info=True)
            return None


def setup_auth(mcp: FastMCP, settings):
    """
    Setup authentication for the MCP server.

    Args:
        mcp: The FastMCP server instance
        settings: Application settings
    """
    if not settings.auth_resource_server_url or not settings.auth_client_id:
        logger.warning("Authentication enabled but missing required configuration")
        return

    authenticator = JWTAuthenticator(
        resource_server_url=settings.auth_resource_server_url,
        client_id=settings.auth_client_id,
        audience=settings.auth_audience or settings.auth_client_id,
        debug=settings.mcp_auth_debug,
    )

    # Add authentication middleware
    @mcp.middleware()
    async def auth_middleware(request, call_next):
        """Middleware to verify JWT tokens."""
        # Extract token from Authorization header
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            logger.warning("Missing or invalid Authorization header")
            return {"error": "Unauthorized", "status": 401}

        token = auth_header[7:]  # Remove "Bearer " prefix
        claims = authenticator.verify_token(token)

        if not claims:
            return {"error": "Invalid or expired token", "status": 401}

        # Add claims to request context
        request.state.user = claims
        return await call_next(request)

    logger.info("Authentication middleware configured")
