"""Client for registering Dataset Linking graphs in Data Model Management."""

from __future__ import annotations

import copy
import logging
import os
from typing import Any, Dict, Optional

import requests

from dl.similarity import BASE_URL, get_access_token


logger = logging.getLogger(__name__)

DMM_DATASET_LINKING_REGISTER_URL = os.getenv(
    "DMM_DATASET_LINKING_REGISTER_URL",
    f"{BASE_URL}/dmm/api/v1/dataset-linking/register",
)
DMM_REQUEST_TIMEOUT_SECONDS = float(os.getenv("DMM_REQUEST_TIMEOUT_SECONDS", "30"))


class DMMRegistrationError(RuntimeError):
    """Raised when DMM cannot register a Dataset Linking graph."""


def prepare_graph_for_dmm(graph: Dict[str, Any]) -> Dict[str, Any]:
    """Return a DMM-safe copy without metadata on existing dataset nodes."""
    payload = copy.deepcopy(graph)
    for node in payload.get("nodes", []) or []:
        if "sc:Dataset" in (node.get("labels") or []):
            node["properties"] = {}
    return payload


def register_graph_in_dmm(
        graph: Dict[str, Any],
        token: Optional[str] = None,
) -> Dict[str, Any]:
    """Register one graph in DMM and return its decoded response."""
    bearer_token = token or get_access_token()
    payload = prepare_graph_for_dmm(graph)
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    logger.info(
        "Registering Dataset Linking graph in DMM",
        extra={
            "dmm_url": DMM_DATASET_LINKING_REGISTER_URL,
            "node_count": len(payload.get("nodes", []) or []),
            "edge_count": len(payload.get("edges", []) or []),
        },
    )
    try:
        response = requests.post(
            DMM_DATASET_LINKING_REGISTER_URL,
            headers=headers,
            json=payload,
            timeout=DMM_REQUEST_TIMEOUT_SECONDS,
            verify=False,
        )
        response.raise_for_status()
    except requests.Timeout as exc:
        logger.error("DMM Dataset Linking registration timed out", exc_info=True)
        raise DMMRegistrationError("DMM registration timed out") from exc
    except requests.RequestException as exc:
        status_code = getattr(exc.response, "status_code", None)
        response_text = getattr(exc.response, "text", "")
        logger.error(
            "DMM Dataset Linking registration failed",
            extra={
                "status_code": status_code,
                "response_text": response_text,
            },
            exc_info=True,
        )
        raise DMMRegistrationError(
            f"DMM registration failed"
            f"{f' with HTTP {status_code}' if status_code else ''}: {response_text}"
        ) from exc

    try:
        response_payload = response.json()
    except ValueError:
        response_payload = {"status_code": response.status_code, "content": response.text}

    logger.info(
        "Dataset Linking graph registered successfully in DMM",
        extra={"status_code": response.status_code},
    )
    return response_payload
