"""OAuth client-credentials authentication for DataGEMS services."""

import os
import threading
import time

import requests


DEFAULT_TOKEN_URL = (
    "https://datagems-dev.scayle.es/oauth/realms/dev/"
    "protocol/openid-connect/token"
)
TOKEN_EXPIRY_MARGIN_SECONDS = 30

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

        try:
            response = requests.post(
                os.getenv("OIDC_TOKEN_URL", DEFAULT_TOKEN_URL),
                data=payload,
                timeout=15,
                verify=False,
            )
            response.raise_for_status()
            token_payload = response.json()
        except (requests.RequestException, ValueError) as exc:
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
        return token
