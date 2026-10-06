"""Keep the most complete profile for each normalized dataset name."""

import argparse
import json
import re
from pathlib import Path

from download_all_profiles import safe_filename


TARGET_NAMES = {
    "19th century knowledge project",
    "encyclopédie, ou dictionnaire raisonné des sciences, des arts et des métiers",
    "encycnet",
}


def normalized_name(value):
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold()


def as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def profile_name(data, path):
    return str(data.get("name") or path.stem).strip()


def completeness_score(data):
    description = str(data.get("description") or "").strip()
    headline = str(data.get("headline") or "").strip()
    keywords = data.get("keywords") or []
    if isinstance(keywords, str):
        keywords = [keywords]

    record_sets = as_list(data.get("recordSet"))
    distributions = as_list(data.get("distribution"))
    required_fields = sum(
        [bool(description), bool(headline), bool(keywords)]
    )

    # Required text fields come first; metadata breaks ties between profiles.
    return (
        required_fields,
        len(description),
        len(headline),
        len(keywords),
        len(record_sets),
        len(distributions),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Deduplicate downloaded DataGEMS profiles by profile name."
    )
    parser.add_argument("--input-dir", default="data/datagems/raw")
    parser.add_argument("--output-dir", default="data/datagems/clean")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    files = [
        path
        for path in input_dir.glob("*.json")
        if path.name != "download_failures.json"
        and not path.name.lower().startswith("cache_")
    ]
    if not files:
        raise RuntimeError(f"No profile JSON files found in {input_dir}.")

    selected = {}
    discarded = []
    for path in files:
        data = json.loads(path.read_text(encoding="utf-8"))
        name = profile_name(data, path)
        key = normalized_name(name)
        candidate = (completeness_score(data), path, data, name)
        current = selected.get(key)
        if current is None or candidate[0] > current[0]:
            if current is not None:
                discarded.append(
                    {
                        "name": current[3],
                        "kept": str(path),
                        "discarded": str(current[1]),
                        "kept_score": candidate[0],
                        "discarded_score": current[0],
                    }
                )
            selected[key] = candidate
        else:
            discarded.append(
                {
                    "name": name,
                    "kept": str(current[1]),
                    "discarded": str(path),
                    "kept_score": current[0],
                    "discarded_score": candidate[0],
                }
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    written_names = set()
    kept_targets = set()
    for _, (_, source_path, data, name) in selected.items():
        filename = safe_filename(name) + ".json"
        if filename in written_names:
            filename = safe_filename(name) + f"__{data.get('@id', source_path.stem)}.json"
        written_names.add(filename)
        (output_dir / filename).write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if normalized_name(name) in TARGET_NAMES:
            kept_targets.add(name)

    (output_dir / "deduplication_report.json").write_text(
        json.dumps(
            {
                "input_profiles": len(files),
                "output_profiles": len(selected),
                "duplicates_removed": len(discarded),
                "kept_language_use_case_profiles": sorted(kept_targets),
                "discarded_profiles": discarded,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Input profiles: {len(files)}")
    print(f"Unique profiles kept: {len(selected)}")
    print(f"Duplicates removed: {len(discarded)}")
    print(f"Output: {output_dir}")


if __name__ == "__main__":
    main()
