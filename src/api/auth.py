"""
OAuth2 authentication and token management for REST APIs

ENTERPRISE OAUTH2 PATTERN:
This implements the standard OAuth2 flow used by most SaaS APIs (Stripe, Salesforce, etc.):
1. Exchange client_id + client_secret for access_token
2. Use access_token in API calls
3. Refresh token when it expires (401 response)
4. Handle refresh_token rotation

SECURITY BEST PRACTICES:
1. Store credentials in AWS Secrets Manager (NEVER hardcode)
2. Rotate tokens regularly (Secrets Manager can do this automatically)
3. Use short-lived access tokens (1 hour typical)
4. Implement token caching to avoid excessive token requests
5. Log token operations (but NEVER log the actual token value)

COMMON PRODUCTION ISSUES:
1. Token expiry mid-job:
   - Symptom: Job runs fine for 1 hour, then starts failing with 401
   - Cause: Access token expired
   - Solution: Proactive refresh before expiry (this class does it)
   - Prevention: Cache token with expiry time, refresh 5 min before expiry

2. Refresh token rotation:
   - Symptom: Refresh fails after using refresh_token once
   - Cause: Some APIs issue new refresh_token on each refresh
   - Solution: Update stored refresh_token after each refresh
   - Example: Salesforce rotates, Google doesn't

3. Rate limiting on token endpoint:
   - Symptom: Getting 429 on /oauth/token
   - Cause: Requesting new tokens too frequently
   - Solution: Cache tokens, don't request on every API call
   - Rule: Max 1 token request per minute per job

4. Client secret rotation:
   - Impact: All jobs start failing immediately
   - Solution: Use AWS Secrets Manager auto-rotation
   - Process: Store old + new secret during rotation window
"""

import logging
import time
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
import json

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class TokenExpiredError(Exception):
    """Raised when token has expired"""
    pass


class AuthenticationError(Exception):
    """Raised when authentication fails"""
    pass


class OAuth2TokenManager:
    """
    Manages OAuth2 tokens with automatic refresh
    
    This class handles the complete OAuth2 flow:
    - Initial token acquisition
    - Token caching
    - Automatic refresh before expiry
    - Token persistence (optional)
    
    USAGE:
        manager = OAuth2TokenManager(
            secret_name="yambo/prod/stripe-api",
            token_url="https://connect.stripe.com/oauth/token"
        )
        access_token = manager.get_access_token()
        # Use access_token in API calls
    """
    
    def __init__(
        self,
        secret_name: str,
        token_url: Optional[str] = None,
        token_expiry_buffer_seconds: int = 300,  # Refresh 5 min before expiry
    ):
        """
        Initialize token manager
        
        Args:
            secret_name: AWS Secrets Manager secret name containing credentials
            token_url: OAuth token endpoint (if different from secret)
            token_expiry_buffer_seconds: Refresh token this many seconds before expiry
        """
        self.secret_name = secret_name
        self.token_url = token_url
        self.token_expiry_buffer = token_expiry_buffer_seconds
        
        # Token cache
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._refresh_token: Optional[str] = None
        
        # Credentials (loaded from Secrets Manager)
        self._client_id: Optional[str] = None
        self._client_secret: Optional[str] = None
        
        # Load credentials
        self._load_credentials()
    
    def _load_credentials(self) -> None:
        """
        Load credentials from AWS Secrets Manager
        
        Expected secret format (JSON):
        {
            "client_id": "ca_...",
            "client_secret": "sk_live_...",
            "token_url": "https://connect.stripe.com/oauth/token",
            "refresh_token": "rt_..." (optional)
        }
        
        PRODUCTION TIP:
        Use different secrets for dev/prod:
        - yambo/dev/stripe-api (uses test keys)
        - yambo/prod/stripe-api (uses live keys)
        
        This prevents accidentally hitting production API from dev jobs.
        """
        secrets_client = boto3.client("secretsmanager")
        
        try:
            response = secrets_client.get_secret_value(SecretId=self.secret_name)
            secret_data = json.loads(response["SecretString"])
            
            self._client_id = secret_data["client_id"]
            self._client_secret = secret_data["client_secret"]
            
            # Token URL can be in secret or passed at init
            if not self.token_url:
                self.token_url = secret_data.get("token_url")
            
            # Some APIs provide refresh token
            self._refresh_token = secret_data.get("refresh_token")
            
            logger.info(
                "Loaded credentials from Secrets Manager",
                extra={"secret_name": self.secret_name},
            )
            
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ResourceNotFoundException":
                raise AuthenticationError(
                    f"Secret not found: {self.secret_name}. "
                    "Did you create the secret in Secrets Manager?"
                )
            elif error_code == "AccessDeniedException":
                raise AuthenticationError(
                    f"Access denied to secret: {self.secret_name}. "
                    "Check IAM role permissions for secretsmanager:GetSecretValue"
                )
            else:
                raise AuthenticationError(f"Failed to load credentials: {e}")
        
        except (KeyError, json.JSONDecodeError) as e:
            raise AuthenticationError(
                f"Invalid secret format in {self.secret_name}. "
                f"Expected JSON with client_id and client_secret. Error: {e}"
            )
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
    )
    def _request_token(
        self,
        grant_type: str = "client_credentials",
        refresh_token: Optional[str] = None,
    ) -> Tuple[str, int, Optional[str]]:
        """
        Request access token from OAuth endpoint
        
        Args:
            grant_type: OAuth grant type (client_credentials or refresh_token)
            refresh_token: Refresh token (if using refresh grant)
        
        Returns:
            Tuple of (access_token, expires_in_seconds, new_refresh_token)
        
        RATE LIMIT NOTE:
        Most token endpoints have generous rate limits (e.g., 100/min),
        but we still implement retry with exponential backoff.
        If you see 429 on token endpoint, you're requesting too frequently.
        """
        if not self.token_url:
            raise AuthenticationError("Token URL not configured")
        
        # Build request payload
        data = {
            "grant_type": grant_type,
        }
        
        if grant_type == "client_credentials":
            data["client_id"] = self._client_id
            data["client_secret"] = self._client_secret
        elif grant_type == "refresh_token":
            if not refresh_token:
                raise AuthenticationError("Refresh token required for refresh grant")
            data["refresh_token"] = refresh_token
            data["client_id"] = self._client_id
            data["client_secret"] = self._client_secret
        
        try:
            logger.debug(f"Requesting token with grant_type={grant_type}")
            
            response = requests.post(
                self.token_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
            response.raise_for_status()
            
            token_data = response.json()
            access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
            new_refresh_token = token_data.get("refresh_token")
            
            logger.info(
                "Successfully obtained access token",
                extra={"expires_in": expires_in, "grant_type": grant_type},
            )
            
            return access_token, expires_in, new_refresh_token
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise AuthenticationError(
                    f"Invalid credentials for {self.secret_name}. "
                    "Check client_id and client_secret in Secrets Manager."
                )
            elif e.response.status_code == 429:
                logger.warning("Rate limited on token endpoint, retrying...")
                raise  # Retry will handle this
            else:
                raise AuthenticationError(f"Token request failed: {e}")
        
        except requests.exceptions.RequestException as e:
            logger.warning(f"Token request failed, retrying: {e}")
            raise  # Retry will handle this
    
    def get_access_token(self) -> str:
        """
        Get valid access token (cached or newly requested)
        
        This is the main method to use. It handles:
        1. Returning cached token if still valid
        2. Refreshing token if close to expiry
        3. Requesting new token if no cache
        
        Returns:
            Valid access token
        
        THREAD SAFETY NOTE:
        This class is NOT thread-safe. In Spark, each executor should have
        its own TokenManager instance (don't share across threads).
        """
        # Check if we have a valid cached token
        if self._access_token and self._token_expiry:
            time_until_expiry = (self._token_expiry - datetime.utcnow()).total_seconds()
            
            if time_until_expiry > self.token_expiry_buffer:
                logger.debug(f"Using cached token (expires in {time_until_expiry:.0f}s)")
                return self._access_token
            else:
                logger.info(
                    f"Token expiring soon ({time_until_expiry:.0f}s), refreshing..."
                )
        
        # Try to refresh if we have refresh token
        if self._refresh_token:
            try:
                access_token, expires_in, new_refresh_token = self._request_token(
                    grant_type="refresh_token",
                    refresh_token=self._refresh_token,
                )
                
                # Update refresh token if API rotated it
                if new_refresh_token:
                    self._refresh_token = new_refresh_token
                    logger.info("Refresh token rotated by API")
                
                self._cache_token(access_token, expires_in)
                return access_token
                
            except AuthenticationError as e:
                logger.warning(f"Token refresh failed: {e}, requesting new token")
        
        # Request new token
        access_token, expires_in, new_refresh_token = self._request_token()
        
        if new_refresh_token:
            self._refresh_token = new_refresh_token
        
        self._cache_token(access_token, expires_in)
        return access_token
    
    def _cache_token(self, access_token: str, expires_in: int) -> None:
        """Cache token with expiry time"""
        self._access_token = access_token
        self._token_expiry = datetime.utcnow() + timedelta(seconds=expires_in)
        
        logger.debug(
            f"Cached token (expires at {self._token_expiry.isoformat()}Z)"
        )
    
    def invalidate_token(self) -> None:
        """
        Invalidate cached token (force refresh on next call)
        
        Call this after receiving 401 from API, indicating token is invalid.
        """
        logger.info("Invalidating cached token")
        self._access_token = None
        self._token_expiry = None


# STRIPE API SIMPLIFICATION:
# Stripe actually uses API keys (not OAuth2), but the pattern is similar.
# For this project, we'll use the API key directly as a "bearer token".
# OAuth2 pattern shown above is for APIs like Salesforce, HubSpot, etc.

class StripeAPIKeyManager:
    """
    Simplified token manager for Stripe API (uses API key directly)
    
    Stripe uses simple API key authentication:
    - Secret key (sk_live_...) for server-side
    - Publishable key (pk_live_...) for client-side (we don't use this)
    
    USAGE:
        manager = StripeAPIKeyManager(secret_name="yambo/prod/stripe-api")
        api_key = manager.get_api_key()
        # Use in Authorization header: Bearer {api_key}
    """
    
    def __init__(self, secret_name: str):
        self.secret_name = secret_name
        self._api_key: Optional[str] = None
        self._load_api_key()
    
    def _load_api_key(self) -> None:
        """
        Load API key from AWS Secrets Manager
        
        Expected secret format (JSON):
        {
            "api_key": "sk_live_...",
            "api_url": "https://api.stripe.com/v1"
        }
        """
        secrets_client = boto3.client("secretsmanager")
        
        try:
            response = secrets_client.get_secret_value(SecretId=self.secret_name)
            secret_data = json.loads(response["SecretString"])
            self._api_key = secret_data["api_key"]
            
            logger.info("Loaded Stripe API key from Secrets Manager")
            
        except Exception as e:
            raise AuthenticationError(f"Failed to load API key: {e}")
    
    def get_api_key(self) -> str:
        """Get API key"""
        if not self._api_key:
            raise AuthenticationError("API key not loaded")
        return self._api_key


# MONITORING TIPS:
#
# 1. Track token refresh frequency:
#    - Normal: 1 refresh per hour (if token valid for 1 hour)
#    - Issue: Multiple refreshes per minute → Token not being cached properly
#
# 2. Alert on authentication errors:
#    - High priority: Credentials invalid (fix immediately)
#    - Medium priority: Token endpoint slow (>5s response time)
#
# 3. Log token operations for auditing:
#    - When token was requested
#    - When token was refreshed
#    - Token expiry time (NOT the actual token value)
