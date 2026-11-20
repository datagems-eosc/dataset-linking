"""OAuth client-credentials authentication for DataGEMS services."""

import logging
import os
import threading
import time
from pathlib import Path

import requests
from dotenv import load_dotenv


logger = logging.getLogger("uvicorn.error")

DEFAULT_TOKEN_URL = (
    "https://datagems-dev.scayle.es/oauth/realms/dev/"
    "protocol/openid-connect/token"
)
TOKEN_EXPIRY_MARGIN_SECONDS = 30

# Load developer-only configuration when running from a checkout. Kubernetes
# and other deployments continue to use their process environment; values
# already present in the environment always take precedence.
load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

_access_token = None
_access_token_expires_at = 0.0
_token_lock = threading.Lock()


class DataGEMSAuthenticationError(RuntimeError):
    """Raised when service credentials are missing or token acquisition fails."""


def reset_access_token_cache() -> None:
    """Clear the process-local token cache, primarily for tests and 401 recovery."""
    global _access_token, _access_token_expires_at
    with _token_lock:
        _access_token = None
        _access_token_expires_at = 0.0


def get_access_token(force_refresh: bool = False) -> str:
    """Return a valid service token, obtaining a new one when required."""
    global _access_token, _access_token_expires_at

    with _token_lock:
        now = time.monotonic()
        if not force_refresh and _access_token and now < _access_token_expires_at:
            return _access_token

        client_id = os.getenv("OIDC_CLIENT_ID")
        client_secret = os.getenv("OIDC_CLIENT_SECRET")
        if not client_id or not client_secret:
            logger.warning(
                "DataGEMS OAuth credentials are not configured: "
                "OIDC_CLIENT_ID=%s OIDC_CLIENT_SECRET=%s",
                "set" if client_id else "missing",
                "set" if client_secret else "missing",
            )
            raise DataGEMSAuthenticationError(
                "OIDC_CLIENT_ID and OIDC_CLIENT_SECRET must be provided by the runtime environment"
            )

        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        scope = os.getenv("OIDC_SCOPE", "datagems").strip()
        if scope:
            payload["scope"] = scope

        token_url = os.getenv("OIDC_TOKEN_URL", DEFAULT_TOKEN_URL)
        logger.info(
            "Requesting DataGEMS OAuth access token: client_id=%s scope=%s reason=%s url=%s",
            client_id,
            scope or "none",
            "forced_refresh" if force_refresh else "missing_or_expired",
            token_url,
        )
        try:
            response = requests.post(
                token_url,
                data=payload,
                timeout=15,
                verify=False,
            )
            response.raise_for_status()
            token_payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            logger.error(
                "Unable to obtain the DataGEMS OAuth access token: status_code=%s",
                status_code or "unavailable",
            )
            raise DataGEMSAuthenticationError("Unable to obtain the DataGEMS access token") from exc

        token = token_payload.get("access_token")
        if not token:
            raise DataGEMSAuthenticationError("The token response does not contain access_token")

        try:
            expires_in = max(float(token_payload.get("expires_in", 0)), 0.0)
        except (TypeError, ValueError):
            expires_in = 0.0
        usable_lifetime = max(expires_in - TOKEN_EXPIRY_MARGIN_SECONDS, 0.0)
        _access_token = token
        _access_token_expires_at = now + usable_lifetime
        logger.info(
            "DataGEMS OAuth access token obtained successfully: "
            "expires_in_seconds=%s renew_after_seconds=%s",
            expires_in,
            usable_lifetime,
        )
        return token
