# dl/similarity.py
import json
from pathlib import Path
from itertools import combinations
from typing import Optional, Tuple, List, Dict, Any

from sentence_transformers import SentenceTransformer

from dl.utils import normalize_keywords, get_DLRepository_path


# Lazy load
_model_short: Optional[SentenceTransformer] = None
_model_long: Optional[SentenceTransformer] = None


def _ensure_models():
    """Load the model only the first time"""
    global _model_short, _model_long
    if _model_short is None or _model_long is None:
        from sentence_transformers import SentenceTransformer
        _model_short = SentenceTransformer("all-MiniLM-L6-v2")   # headlines
        _model_long = SentenceTransformer("all-mpnet-base-v2")   # descriptions


def compute_similarities(
    folder_path: Optional[str],
    kw_weight: float = 0.6,
    desc_weight: float = 0.3,
    head_weight: float = 0.1,
    threshold: float = 30.0
) -> Tuple[Optional[str], List[Dict[str, Any]], Optional[bool]]:
    """
    Compute pairwise similarities between all JSON files in a folder.
    Returns (error, similarities, from_DLRepository)
    """
    folder = Path(folder_path) if folder_path else (Path.home() / "Desktop" / "Profiles")
    DLRepository_path = get_DLRepository_path(folder, kw_weight, desc_weight, head_weight)

    # --- DLRepository check ---
    if DLRepository_path.exists():
        try:
            with open(DLRepository_path, "r", encoding="utf-8") as f:
                similarities = json.load(f)
            print(f"🟢 DLRepository hit: {DLRepository_path.name}")

            for s in similarities:
                s["passes_threshold"] = s.get("combined_similarity", 0) >= threshold

            return None, similarities, True
        except Exception as e:
            print(f"⚠️ DLRepository read failed ({e}), recalculating...")

    # --- Validate folder ---
    if not folder.exists():
        return f"❌ Folder not found: {folder}", [], False

    json_files = list(folder.glob("*.json"))
    if not json_files:
        return "⚠️ No .json files found.", [], False

    # --- Load all profiles ---
    file_data: Dict[str, Dict[str, Any]] = {}
    for file in json_files:
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
            keywords = data.get('keywords', [])
            description = data.get('description', '')
            headline = data.get('headline', '')
            pid = data.get("@id", '')
            file_data[file.name] = {
                "keywords": normalize_keywords(keywords) if isinstance(keywords, list) else set(),
                "description": description,
                "headline": headline,
                "id": pid
            }
        except json.JSONDecodeError:
            continue

    # --- Prepare models ---
    _ensure_models()
    from sentence_transformers import util
    similarities = []

    # --- Compute pairwise ---
    for file1, file2 in combinations(file_data.keys(), 2):
        f1, f2 = file_data[file1], file_data[file2]

        # Jaccard for keywords
        kw1, kw2 = f1["keywords"], f2["keywords"]
        common = kw1 & kw2
        union = kw1 | kw2
        unique_to_1 = kw1 - kw2
        unique_to_2 = kw2 - kw1
        keyword_similarity = (len(common) / len(union) * 100) if union else 0

        # Text similarities
        emb_desc1 = _model_long.encode(f1["description"], convert_to_tensor=True)
        emb_desc2 = _model_long.encode(f2["description"], convert_to_tensor=True)
        emb_head1 = _model_short.encode(f1["headline"], convert_to_tensor=True)
        emb_head2 = _model_short.encode(f2["headline"], convert_to_tensor=True)

        desc_similarity = util.cos_sim(emb_desc1, emb_desc2).item()
        head_similarity = util.cos_sim(emb_head1, emb_head2).item()
        desc_similarity = max(0.0, min(1.0, desc_similarity))
        head_similarity = max(0.0, min(1.0, head_similarity))

        combined_score = (
            kw_weight * (keyword_similarity / 100) +
            desc_weight * desc_similarity +
            head_weight * head_similarity
        ) * 100

        similarities.append({
            "dataprofile1": file1,
            "dataprofile2": file2,
            "id1": f1.get("id"),
            "id2": f2.get("id"),
            "keywords_similarity": round(keyword_similarity, 2),
            "description_similarity": round(desc_similarity * 100, 2),
            "headline_similarity": round(head_similarity * 100, 2),
            "combined_similarity": round(combined_score, 2),
            "common_keywords": ", ".join(sorted(common)),
            "common_count": len(common),
            "unique_to_1": ", ".join(sorted(unique_to_1)),
            "unique_to_2": ", ".join(sorted(unique_to_2)),
            "passes_threshold": combined_score >= threshold
        })

    # --- Save DLRepository ---
    try:
        with open(DLRepository_path, "w", encoding="utf-8") as f:
            json.dump(similarities, f, ensure_ascii=False, indent=4)
        print(f"💾 DLRepository saved: {DLRepository_path.name}")
    except Exception as e:
        print(f"⚠️ Failed to save DLRepository: {e}")

    return None, similarities, None