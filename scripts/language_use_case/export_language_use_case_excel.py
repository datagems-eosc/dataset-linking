"""Export the language-use-case pairwise comparisons for the PDF renderer."""

import json
import shutil
import sys
import tempfile
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import dl.similarity as similarity


PROFILE_DIR = Path("data/language_use_case/raw")
OUTPUT_XLSX = Path("reports/language_use_case/SimilarityReport_language_use_case.xlsx")
WEIGHT_CONFIGURATIONS = (
    (0.6, 0.3, 0.1),
    (0.3, 0.6, 0.1),
    (0.33, 0.33, 0.33),
)


def _format_description_chunks(chunks):
    if not chunks:
        return "N/A"
    return " | ".join(
        f"[{item.get('similarity', 0):.2f}] "
        f"{item.get('chunk1', '')} <-> {item.get('chunk2', '')}"
        for item in chunks
    )


def _profile_names():
    names = {}
    for path in sorted(PROFILE_DIR.glob("*.json")):
        if path.name.lower().startswith("cache_"):
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        names[data.get("@id", path.name)] = data.get("name", path.stem)
    return names


def _rows(similarities):
    return [
        {
            "ID dataset 1": item["id1"],
            "Dataset 1": item["dataprofile1"],
            "ID dataset 2": item["id2"],
            "Dataset 2": item["dataprofile2"],
            "Common Keywords": item["common_keywords"],
            "Unique to DS1": item["unique_to_1"],
            "Unique to DS2": item["unique_to_2"],
            "Top 3 Desc Chunks": _format_description_chunks(
                item.get("description_top_chunks", [])
            ),
            "Top 3 Headline Chunks": "N/A",
            "Sim Keywords (%)": item["keywords_similarity"],
            "Sim Description (%)": item["description_similarity"],
            "Sim Headline (%)": item["headline_similarity"],
            "Final Sim (%)": item["combined_similarity"],
        }
        for item in similarities
    ]


def main():
    expected = 3
    actual = len(
        [
            path
            for path in PROFILE_DIR.glob("*.json")
            if not path.name.lower().startswith("cache_")
        ]
    )
    if actual != expected:
        raise RuntimeError(
            f"Expected {expected} profile JSON files in {PROFILE_DIR}, found {actual}."
        )

    profile_names = _profile_names()
    print(f"Analyzing {len(profile_names)} language-use-case profiles...")

    # Load the embedding models once. Each weight configuration gets an
    # isolated folder because the current local engine writes cache JSON files
    # into the input folder and does not ignore them in its local scan.
    similarity._ensure_models()

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        for kw_weight, desc_weight, head_weight in WEIGHT_CONFIGURATIONS:
            sheet_name = f"{kw_weight:g}_{desc_weight:g}_{head_weight:g}"
            with tempfile.TemporaryDirectory(
                prefix=f"language_use_case_{sheet_name}_"
            ) as temp_dir:
                analysis_dir = Path(temp_dir)
                for profile_path in PROFILE_DIR.glob("*.json"):
                    if profile_path.name.lower().startswith("cache_"):
                        continue
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
                frame = pd.DataFrame(_rows(results))
                frame.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"{sheet_name}: {len(frame)} comparisons")

    print(f"Created {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
