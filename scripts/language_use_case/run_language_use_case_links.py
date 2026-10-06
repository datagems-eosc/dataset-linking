"""Compute links involving the three language-use-case datasets only."""

import json
import argparse
import shutil
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import dl.similarity as similarity


DEFAULT_PROFILE_DIR = Path("data/datagems/raw/all_profiles")
DEFAULT_OUTPUT_DIR = Path("reports/language_use_case/results")
TARGET_IDS = {
    "d84d1a2e-127d-4393-91d0-afb7e4fd9c68",
    "d5c34990-9acf-4349-91d4-924f58565922",
    "07382b91-5bc5-42f9-8391-33adc2460c19",
}
WEIGHT_CONFIGURATIONS = (
    (0.6, 0.3, 0.1),
    (0.3, 0.6, 0.1),
    (0.3, 0.3, 0.3),
)


def _profile_files_in(profile_dir):
    return [
        path
        for path in profile_dir.glob("*.json")
        if not path.name.lower().startswith("cache_")
        and path.name != "download_failures.json"
        and path.name != "deduplication_report.json"
    ]


def _profile_ids(profile_files):
    ids = set()
    for path in profile_files:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("@id") in TARGET_IDS:
            ids.add(data["@id"])
    return ids


def main():
    parser = argparse.ArgumentParser(
        description="Compute links involving language-use-case datasets only."
    )
    parser.add_argument(
        "--profiles-dir",
        default=str(DEFAULT_PROFILE_DIR),
        help="Folder containing downloaded profile JSON files.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Folder for the filtered result JSON files.",
    )
    args = parser.parse_args()

    profile_dir = Path(args.profiles_dir)
    output_dir = Path(args.output_dir)
    profiles = _profile_files_in(profile_dir)
    if len(profiles) < 2:
        raise RuntimeError(f"Expected profile JSON files in {profile_dir}.")

    found_target_ids = _profile_ids(profiles)
    missing_targets = sorted(TARGET_IDS - found_target_ids)
    if missing_targets:
        raise RuntimeError(f"Missing language-use-case profiles: {missing_targets}")

    output_dir.mkdir(parents=True, exist_ok=True)
    similarity._ensure_models()

    for kw_weight, desc_weight, head_weight in WEIGHT_CONFIGURATIONS:
        label = f"{kw_weight:g}_{desc_weight:g}_{head_weight:g}"

        # The local similarity engine writes cache JSON files into its input
        # directory. A fresh directory prevents a prior run from becoming an
        # accidental input profile.
        with tempfile.TemporaryDirectory(prefix=f"language_links_{label}_") as temp_dir:
            analysis_dir = Path(temp_dir)
            for profile_path in profiles:
                shutil.copy2(profile_path, analysis_dir / profile_path.name)

            error, results, _ = similarity.compute_similarities(
                folder_path=str(analysis_dir),
                kw_weight=kw_weight,
                desc_weight=desc_weight,
                head_weight=head_weight,
                threshold=0,
                use_api=False,
                include_description_chunks=True,
            )
            if error:
                raise RuntimeError(error)

        target_results = [
            item
            for item in results
            if item.get("id1") in TARGET_IDS or item.get("id2") in TARGET_IDS
        ]
        output_path = output_dir / f"{label}.json"
        output_path.write_text(
            json.dumps(target_results, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"{label}: {len(target_results)} target links -> {output_path}")


if __name__ == "__main__":
    main()
