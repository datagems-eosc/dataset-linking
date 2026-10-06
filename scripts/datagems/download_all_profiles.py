"""Download all DataGEMS profiles using the application's API configuration."""

import argparse
import json
import re
import sys
import time
from pathlib import Path

import requests
import urllib3

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import dl.similarity as similarity


PAGE_SIZE = 100
RETRIES = 3
INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*]')


def profile_id(item):
    nodes = item.get("nodes", []) or []
    return nodes[0].get("id") if nodes else None


def profile_name(item):
    nodes = item.get("nodes", []) or []
    if not nodes:
        return "unnamed_profile"
    return nodes[0].get("properties", {}).get("name") or "unnamed_profile"


def safe_filename(name):
    """Make an API profile name valid as a Windows filename."""
    cleaned = INVALID_FILENAME_CHARS.sub("_", str(name))
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    if not cleaned:
        cleaned = "unnamed_profile"
    if cleaned.upper() in {"CON", "PRN", "AUX", "NUL"}:
        cleaned = f"_{cleaned}"
    return cleaned[:180]


def fetch_all_profiles(token):
    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    profiles = {}
    offset = 0
    total = None

    while total is None or offset < total:
        response = requests.get(
            similarity.SEARCH_URL,
            headers=headers,
            params={
                "properties": "name",
                "count": PAGE_SIZE,
                "offset": offset,
            },
            timeout=30,
            verify=False,
        )
        response.raise_for_status()
        payload = similarity.fix_encoding(response.json())
        page = payload.get("datasets", []) or []
        total = int(payload.get("total", offset + len(page)))

        for item in page:
            dataset_id = profile_id(item)
            if dataset_id:
                profiles[dataset_id] = item

        if not page:
            break
        offset += len(page)
        print(f"Found profiles: {offset}/{total}")

    return profiles


def download_profile(session, headers, dataset_id):
    url = f"{similarity.DETAIL_URL}{dataset_id}?format=croissant"
    last_error = None
    for attempt in range(1, RETRIES + 1):
        try:
            response = session.get(
                url,
                headers=headers,
                timeout=40,
                verify=False,
            )
            response.raise_for_status()
            payload = similarity.fix_encoding(response.json())
            return payload.get("dataset", payload)
        except Exception as error:
            last_error = error
            if attempt < RETRIES:
                time.sleep(2 * attempt)
    raise last_error


def main():
    parser = argparse.ArgumentParser(
        description="Download all DataGEMS dataset profiles as JSON files."
    )
    parser.add_argument(
        "--output-dir",
        default="data/datagems/raw/all_profiles",
        help="Destination folder (default: data/datagems/raw/all_profiles)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    token = similarity.get_access_token()
    search_profiles = fetch_all_profiles(token)
    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    session = requests.Session()
    used_names = {}
    failures = []

    for index, (dataset_id, item) in enumerate(search_profiles.items(), 1):
        api_name = profile_name(item)
        base_name = safe_filename(api_name)
        filename = f"{base_name}.json"

        if filename in used_names:
            filename = f"{base_name}__{dataset_id}.json"
        used_names[filename] = dataset_id

        try:
            profile = download_profile(session, headers, dataset_id)
            (output_dir / filename).write_text(
                json.dumps(profile, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"[{index}/{len(search_profiles)}] Saved: {filename}")
        except Exception as error:
            failures.append(
                {
                    "id": dataset_id,
                    "name": api_name,
                    "error": str(error),
                }
            )
            print(f"[{index}/{len(search_profiles)}] ERROR {api_name}: {error}")

    if failures:
        (output_dir / "download_failures.json").write_text(
            json.dumps(failures, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    print(
        f"Completed: {len(search_profiles) - len(failures)} downloaded, "
        f"{len(failures)} failed. Output: {output_dir}"
    )


if __name__ == "__main__":
    main()
