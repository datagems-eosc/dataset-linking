import argparse
import io
import os
import sys
from collections import Counter
from typing import Any, Dict, List, Optional

# Avoid shadowing Python's stdlib token module with dl/token.py when this script
# is executed directly from the dl directory.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR in sys.path:
    sys.path.remove(SCRIPT_DIR)

import requests
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


BASE_URL = "https://datagems-dev.scayle.es"
AUTH_URL = f"{BASE_URL}/oauth/realms/dev/protocol/openid-connect/token"
SEARCH_URL = f"{BASE_URL}/dmm/api/v1/dataset/search"
DETAIL_URL = f"{BASE_URL}/dmm/api/v1/dataset/get/"


def fix_encoding(data: Any) -> Any:
    if isinstance(data, str):
        try:
            return data.encode("latin-1").decode("utf-8")
        except Exception:
            return data
    if isinstance(data, list):
        return [fix_encoding(item) for item in data]
    if isinstance(data, dict):
        return {key: fix_encoding(value) for key, value in data.items()}
    return data


def get_access_token() -> str:
    payload = {
        "grant_type": "password",
        "client_id": "swagger-client",
        "username": "dg-user-admin",
        "password": "dg-user-admin",
        "scope": "datagems",
    }
    response = requests.post(AUTH_URL, data=payload, timeout=15, verify=False)
    response.raise_for_status()
    return response.json().get("access_token")


def search_datasets(token: str, status: Optional[str] = None, count: int = 200) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    params = {"properties": "name", "count": count}
    if status:
        params["dataset_status"] = status

    response = requests.get(SEARCH_URL, headers=headers, params=params, timeout=30, verify=False)
    response.raise_for_status()
    return fix_encoding(response.json()).get("datasets", [])


def dataset_id(item: Dict[str, Any]) -> Optional[str]:
    nodes = item.get("nodes", []) or []
    if not nodes:
        return None
    return nodes[0].get("id")


def dataset_name(item: Dict[str, Any]) -> str:
    nodes = item.get("nodes", []) or []
    if not nodes:
        return "unnamed"
    return nodes[0].get("properties", {}).get("name", "unnamed")


def fetch_detail_status(token: str, ds_id: str) -> str:
    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    response = requests.get(f"{DETAIL_URL}{ds_id}?format=croissant", headers=headers, timeout=25, verify=False)
    response.raise_for_status()
    dataset = fix_encoding(response.json()).get("dataset", {}) or {}
    return str(dataset.get("status", "missing_status") or "missing_status").lower()


def count_detail_statuses(token: str, datasets: List[Dict[str, Any]]) -> Counter:
    statuses = Counter()
    total = len(datasets)

    for index, item in enumerate(datasets, 1):
        ds_id = dataset_id(item)
        name = dataset_name(item)
        if not ds_id:
            statuses["missing_id"] += 1
            continue

        try:
            status = fetch_detail_status(token, ds_id)
        except Exception as exc:
            status = "detail_error"
            print(f"[{index}/{total}] ⚠️ Detail error for {name} ({ds_id}): {exc}")

        statuses[status] += 1
        print(f"[{index}/{total}] {name} ({ds_id}) -> detail status: {status}")

    return statuses


def print_counter(title: str, counter: Counter) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    for status, value in sorted(counter.items()):
        print(f"{status}: {value}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Count platform datasets and Croissant detail statuses.")
    parser.add_argument("--count", type=int, default=200, help="Maximum number of datasets to request from search API.")
    parser.add_argument(
        "--skip-details",
        action="store_true",
        help="Only count search API results without downloading Croissant details.",
    )
    args = parser.parse_args()

    token = get_access_token()
    print("🔑 Token obtained.")

    all_search_results = search_datasets(token, count=args.count)
    ready_search_results = search_datasets(token, status="ready", count=args.count)

    print("\nSearch API counts")
    print("-----------------")
    print(f"All datasets returned by search: {len(all_search_results)}")
    print(f"Datasets returned by search with dataset_status=ready: {len(ready_search_results)}")

    if args.skip_details:
        return

    print("\nChecking Croissant detail status for all search results...")
    all_detail_statuses = count_detail_statuses(token, all_search_results)
    print_counter("Croissant detail statuses for all search results", all_detail_statuses)

    print("\nChecking Croissant detail status for search results filtered as ready...")
    ready_detail_statuses = count_detail_statuses(token, ready_search_results)
    print_counter("Croissant detail statuses for search dataset_status=ready results", ready_detail_statuses)


if __name__ == "__main__":
    main()
