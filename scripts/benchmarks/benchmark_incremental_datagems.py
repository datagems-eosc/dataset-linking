"""Benchmark full versus incremental Dataset Linking analysis.

The input profiles must be downloaded from DataGEMS beforehand. The benchmark
uses the same ordered 31 profiles for every run and writes a human-readable
TXT report with profile names, pair counts, cache hits and timings.
"""

import argparse
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dl import similarity
from dl.link_comparison import build_composite_datalinking_elements
from dl.reports import build_croissant_report


WEIGHTS = (0.6, 0.3, 0.1)
PROFILE_COUNT_BEFORE = 30
PROFILE_COUNT_AFTER = 31


def profile_files(profile_dir):
    return sorted(
        path
        for path in profile_dir.glob("*.json")
        if path.name != "deduplication_report.json"
        and not path.name.startswith("cache_")
    )


def read_profile(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data.get("dataset"), dict):
        data = data["dataset"]
    return data


def copy_profiles(paths, destination):
    destination.mkdir(parents=True, exist_ok=True)
    for path in paths:
        shutil.copy2(path, destination / path.name)


def run_similarity(folder):
    started = time.perf_counter()
    error, results, from_cache = similarity.compute_similarities(
        folder_path=str(folder),
        kw_weight=WEIGHTS[0],
        desc_weight=WEIGHTS[1],
        head_weight=WEIGHTS[2],
        threshold=30,
        use_api=False,
        include_description_chunks=True,
        keyword_method="jaccard",
    )
    elapsed = time.perf_counter() - started
    if error:
        raise RuntimeError(error)
    return {
        "elapsed": elapsed,
        "results": results,
        "from_cache": from_cache,
        "stats": dict(similarity.LAST_PAIRWISE_STATS),
    }


def build_links(folder, similarities):
    return build_croissant_report(str(folder), {
        "keywords": WEIGHTS[0],
        "description": WEIGHTS[1],
        "headline": WEIGHTS[2],
        "threshold": 30,
    }, similarities)["links"]


def profile_chunks(paths):
    chunks = {}
    labels = {}
    for path in paths:
        data = read_profile(path)
        profile_id = str(data.get("@id") or path.name).strip().lower()
        chunks[profile_id] = similarity._split_chunks(data.get("description", ""))
        labels[profile_id] = str(data.get("name") or profile_id)
    return chunks, labels


def run_composites(folder, similarities, paths, caches):
    links = build_links(folder, similarities)
    chunks, labels = profile_chunks(paths)
    started = time.perf_counter()
    result = build_composite_datalinking_elements(
        links,
        relation_threshold=0.5,
        base_threshold=0.3,
        profile_chunks=chunks,
        profile_labels=labels,
        profile_chunk_cache=caches["profile"],
        original_chunk_cache=caches["original"],
    )
    elapsed = time.perf_counter() - started
    return {
        "elapsed": elapsed,
        "links": len(links),
        "comparisons": len(result.get("comparisons", [])),
        "recursive_comparisons": len(result.get("recursive_comparisons", [])),
        "final_composites": len(result.get("final_composites", [])),
        "profile_cache_entries": len(caches["profile"]),
        "original_cache_entries": len(caches["original"]),
    }


def format_seconds(value):
    return f"{value:.3f} seconds ({value / 60:.2f} minutes)"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profiles-dir",
        default="data/datagems/clean",
        help="Folder containing profiles downloaded from DataGEMS.",
    )
    parser.add_argument(
        "--report",
        default="reports/benchmarks/incremental_datagems_benchmark.txt",
        help="TXT report path.",
    )
    args = parser.parse_args()

    source_dir = Path(args.profiles_dir)
    paths = profile_files(source_dir)
    if len(paths) < PROFILE_COUNT_AFTER:
        raise RuntimeError(
            f"Expected at least {PROFILE_COUNT_AFTER} profiles in {source_dir}, "
            f"found {len(paths)}."
        )
    selected = paths[:PROFILE_COUNT_AFTER]

    report_lines = [
        "DATASET LINKING INCREMENTAL CACHE BENCHMARK",
        "================================================",
        "",
        "Source: profiles downloaded from DataGEMS through the API",
        f"Source folder: {source_dir.resolve()}",
        f"Weights: keywords={WEIGHTS[0]}, description={WEIGHTS[1]}, headline={WEIGHTS[2]}",
        "Keyword method: jaccard",
        "Description chunks: enabled",
        "",
        "PROFILES USED IN ALL RUNS",
        "--------------------------",
    ]
    for index, path in enumerate(selected, 1):
        data = read_profile(path)
        report_lines.append(
            f"{index:02d}. {data.get('name', path.name)} | "
            f"id={data.get('@id', path.stem)} | file={path.name}"
        )

    report_lines.extend([
        "",
        "The first 30 profiles are used in the initial run.",
        "Profile 31 is then added to exactly the same set.",
        "",
    ])

    with tempfile.TemporaryDirectory(prefix="datagems_incremental_benchmark_") as temp:
        root = Path(temp)
        baseline_30 = root / "baseline_30"
        baseline_31 = root / "baseline_31"
        incremental = root / "incremental"
        copy_profiles(selected[:PROFILE_COUNT_BEFORE], baseline_30)
        copy_profiles(selected, baseline_31)
        copy_profiles(selected[:PROFILE_COUNT_BEFORE], incremental)

        # Baseline: separate folders force a complete recalculation in both runs.
        baseline_similarity_30 = run_similarity(baseline_30)
        baseline_composite_30 = run_composites(
            baseline_30,
            baseline_similarity_30["results"],
            selected[:PROFILE_COUNT_BEFORE],
            {"profile": {}, "original": {}},
        )
        baseline_similarity_31 = run_similarity(baseline_31)
        baseline_composite_31 = run_composites(
            baseline_31,
            baseline_similarity_31["results"],
            selected,
            {"profile": {}, "original": {}},
        )

        # Incremental: the second run shares the pairwise and composite caches.
        incremental_caches = {"profile": {}, "original": {}}
        incremental_similarity_30 = run_similarity(incremental)
        incremental_composite_30 = run_composites(
            incremental,
            incremental_similarity_30["results"],
            selected[:PROFILE_COUNT_BEFORE],
            incremental_caches,
        )
        copy_profiles([selected[PROFILE_COUNT_AFTER - 1]], incremental)
        incremental_similarity_31 = run_similarity(incremental)
        incremental_composite_31 = run_composites(
            incremental,
            incremental_similarity_31["results"],
            selected,
            incremental_caches,
        )

    report_lines.extend([
        "SIMILARITY: FULL RECALCULATION BASELINE",
        "----------------------------------------",
        f"30 profiles: {len(baseline_similarity_30['results'])} pairs, "
        f"{format_seconds(baseline_similarity_30['elapsed'])}",
        f"31 profiles: {len(baseline_similarity_31['results'])} pairs, "
        f"{format_seconds(baseline_similarity_31['elapsed'])}",
        "Pair cache is intentionally not shared between these runs.",
        "",
        "SIMILARITY: INCREMENTAL CACHE",
        "------------------------------",
        f"30 profiles: {len(incremental_similarity_30['results'])} pairs, "
        f"{format_seconds(incremental_similarity_30['elapsed'])}",
        f"  cache misses={incremental_similarity_30['stats'].get('pair_cache_misses')}, "
        f"cache hits={incremental_similarity_30['stats'].get('pair_cache_hits')}",
        f"31 profiles: {len(incremental_similarity_31['results'])} pairs, "
        f"{format_seconds(incremental_similarity_31['elapsed'])}",
        f"  cache misses={incremental_similarity_31['stats'].get('pair_cache_misses')}, "
        f"cache hits={incremental_similarity_31['stats'].get('pair_cache_hits')}",
        f"Similarity time saved on the second run: "
        f"{format_seconds(baseline_similarity_31['elapsed'] - incremental_similarity_31['elapsed'])}",
        "",
        "BUILD COMPOSITES: FULL RECALCULATION BASELINE",
        "-----------------------------------------------",
        f"30 profiles: links={baseline_composite_30['links']}, "
        f"final composites={baseline_composite_30['final_composites']}, "
        f"{format_seconds(baseline_composite_30['elapsed'])}",
        f"31 profiles: links={baseline_composite_31['links']}, "
        f"final composites={baseline_composite_31['final_composites']}, "
        f"{format_seconds(baseline_composite_31['elapsed'])}",
        "Composite caches are intentionally not shared between these runs.",
        "",
        "BUILD COMPOSITES: INCREMENTAL CACHE",
        "-----------------------------------",
        f"30 profiles: links={incremental_composite_30['links']}, "
        f"final composites={incremental_composite_30['final_composites']}, "
        f"{format_seconds(incremental_composite_30['elapsed'])}",
        f"  profile cache entries={incremental_composite_30['profile_cache_entries']}, "
        f"original chunk cache entries={incremental_composite_30['original_cache_entries']}",
        f"31 profiles: links={incremental_composite_31['links']}, "
        f"final composites={incremental_composite_31['final_composites']}, "
        f"{format_seconds(incremental_composite_31['elapsed'])}",
        f"  profile cache entries={incremental_composite_31['profile_cache_entries']}, "
        f"original chunk cache entries={incremental_composite_31['original_cache_entries']}",
        f"Composite time saved on the second run: "
        f"{format_seconds(baseline_composite_31['elapsed'] - incremental_composite_31['elapsed'])}",
        "",
        "INTERPRETATION",
        "--------------",
        "The baseline recalculates the 30-profile result and then recalculates the 31-profile result from scratch.",
        "The incremental run keeps the same 30 profiles, adds the same profile 31, and reuses existing pairwise and composite chunk calculations.",
        "The grouping stage of build composites still runs again because a new profile can change group membership; expensive profile-pair chunk work is reused.",
    ])

    Path(args.report).write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(f"Benchmark report written to {args.report}")


if __name__ == "__main__":
    main()
