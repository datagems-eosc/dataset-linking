# dl/reports.py
import uuid
import os, json

def build_croissant_report(folder_path, weights, similarities, file_data=None):
    """
    Build a Croissant-like semantic report containing:
      - all analyzed profiles as DLElements
      - similarity links between them
    """
    if file_data is None:
        file_data = {}
        for fn in os.listdir(folder_path):
            if not fn.lower().endswith(".json"):
                continue
            path = os.path.join(folder_path, fn)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    dp = json.load(f)
                file_data[fn] = {
                    "description": dp.get("description", "") or "",
                    "headline": dp.get("headline", "") or "",
                    "keywords": dp.get("keywords", []) or [],
                }
            except (OSError, json.JSONDecodeError) as e:
                file_data[fn] = {"description": "", "headline": "", "keywords": []}
                print(f"⚠️ Failed to load {path}: {e}")
    report = {
        "@context": "http://mlcommons.org/croissant/",
        "@type": "DatasetSimilarityReport",
        "analyzedFolder": str(folder_path),
        "weights": weights,
        "elements": [],
        "links": []
    }

    # --- Elements ---
    unique_dataset = set()
    for s in similarities:
        unique_dataset.add(s["dataprofile1"])
        unique_dataset.add(s["dataprofile2"])

    for dataset_name in sorted(unique_dataset):
        info = file_data.get(dataset_name, {})
        element = {
            "@type": "DLElement",
            "@id": f"profile:{dataset_name.replace('.json', '')}",
            "name": dataset_name,
            "description": info.get("description", ""),
            "keywords": sorted(list(info.get("keywords", []))),
            "headline": info.get("headline", ""),
            "source": {
                "@type": "DataDownload",
                "contentUrl": f"file:///{folder_path}/{dataset_name}",
                "encodingFormat": "application/json"
            }
        }
        report["elements"].append(element)

    # --- Links ---
    for s in similarities:
        link = {
            "@type": "SimilarityLink",
            "@id": f"link:{uuid.uuid4()}",
            "dataprofile1": f"profile:{s['dataprofile1'].replace('.json', '')}",
            "dataprofile2": f"profile:{s['dataprofile2'].replace('.json', '')}",
            "dataprofile1_id": s.get("id1"),
            "dataprofile2_id": s.get("id2"),
            "metrics": {
                "keywords_similarity": s["keywords_similarity"],
                "description_similarity": s["description_similarity"],
                "headline_similarity": s["headline_similarity"],
                "combined_similarity": s["combined_similarity"]
            },
            "common_keywords": [
                kw.strip()
                for kw in s["common_keywords"].split(",")
                if kw.strip()
            ],
            "unique_to_1": [
                kw.strip()
                for kw in s.get("unique_to_1", "").split(",")
                if kw.strip()
            ],
            "unique_to_2": [
                kw.strip()
                for kw in s.get("unique_to_2", "").split(",")
                if kw.strip()
            ]
        }
        report["links"].append(link)

    return report
