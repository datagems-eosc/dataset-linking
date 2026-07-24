import math
import uuid
from itertools import combinations
from typing import Any, Dict, List, Optional, Set


METRIC_KEYS = [
    "keywords_similarity",
    "description_similarity",
    "headline_similarity",
    "combined_similarity",
]


def _as_set(values: Any) -> Set[str]:
    if not values:
        return set()
    if isinstance(values, str):
        values = [v.strip() for v in values.split(",")]
    return {str(v).strip().lower() for v in values if str(v).strip()}


def _jaccard(set1: Set[str], set2: Set[str]) -> float:
    if not set1 and not set2:
        return 0.0
    union = set1 | set2
    if not union:
        return 0.0
    return len(set1 & set2) / len(union)


def _profile_entries(link: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {
            "id": str(link.get("dataprofile1ref") or "").strip(),
            "name": str(link.get("dp1Name") or "").strip(),
        },
        {
            "id": str(link.get("dataprofile2ref") or "").strip(),
            "name": str(link.get("dp2Name") or "").strip(),
        },
    ]


def _profile_key(profile: Dict[str, Any]) -> str:
    return (profile.get("id") or profile.get("name") or "").strip().lower()


def _shared_profile_details(link1: Dict[str, Any], link2: Dict[str, Any]) -> List[Dict[str, Any]]:
    profiles1 = {_profile_key(profile): profile for profile in _profile_entries(link1) if _profile_key(profile)}
    profiles2 = {_profile_key(profile): profile for profile in _profile_entries(link2) if _profile_key(profile)}
    shared = []

    profile_evidence1 = link1.get("profile_evidence", {}) or {}
    profile_evidence2 = link2.get("profile_evidence", {}) or {}

    for key in sorted(set(profiles1) & set(profiles2)):
        profile = profiles1[key]
        evidence = profile_evidence1.get(profile.get("id")) or profile_evidence2.get(profile.get("id")) or {}
        detail = {
            "id": profile.get("id"),
            "name": profile.get("name") or profile.get("id"),
        }
        if evidence:
            detail["files"] = evidence.get("files", [])
            detail["columns"] = evidence.get("columns", [])
            detail["samples"] = evidence.get("samples", [])
        shared.append(detail)

    return shared


def _link_label(link: Dict[str, Any]) -> str:
    return f"{link.get('dp1Name', '')} <-> {link.get('dp2Name', '')}".strip()


def _link_summary(link: Dict[str, Any]) -> Dict[str, Any]:
    metrics = link.get("metrics") or {}
    return {
        "dp1Name": link.get("dp1Name"),
        "dp2Name": link.get("dp2Name"),
        "dataprofile1ref": link.get("dataprofile1ref"),
        "dataprofile2ref": link.get("dataprofile2ref"),
        "combined_similarity": metrics.get("combined_similarity"),
        "field_usage": metrics.get("field_usage"),
        "effective_weights": metrics.get("effective_weights"),
    }


def _metric_profile_similarity(link1: Dict[str, Any], link2: Dict[str, Any]) -> float:
    metrics1 = link1.get("metrics", {}) or {}
    metrics2 = link2.get("metrics", {}) or {}

    squared_distance = 0.0
    count = 0
    for key in METRIC_KEYS:
        if key not in metrics1 or key not in metrics2:
            continue
        try:
            value1 = float(metrics1[key]) / 100.0
            value2 = float(metrics2[key]) / 100.0
        except (TypeError, ValueError):
            continue
        squared_distance += (value1 - value2) ** 2
        count += 1

    if count == 0:
        return 0.0

    distance = math.sqrt(squared_distance / count)
    return max(0.0, min(1.0, 1.0 - distance))


def _chunk_texts(link: Dict[str, Any]) -> Set[str]:
    texts = set()
    for item in link.get("description_top_chunks", []) or []:
        if not isinstance(item, dict):
            continue
        for key in ("chunk1", "chunk2"):
            value = str(item.get(key, "")).strip().lower()
            if value:
                texts.add(value)
    return texts


def _refinement_values(link: Dict[str, Any], key: str) -> Set[str]:
    evidence = link.get("refinement_evidence", {}) or {}
    return _as_set(evidence.get(key, []))


def _refinement_overlap(link1: Dict[str, Any], link2: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    files1 = _refinement_values(link1, "matching_files")
    files2 = _refinement_values(link2, "matching_files")
    columns1 = _refinement_values(link1, "matching_columns")
    columns2 = _refinement_values(link2, "matching_columns")
    samples1 = _refinement_values(link1, "matching_samples")
    samples2 = _refinement_values(link2, "matching_samples")

    if not any([files1, files2, columns1, columns2, samples1, samples2]):
        return None

    return {
        "shared_files": sorted(files1 & files2),
        "common_files_overlap": round(_jaccard(files1, files2), 4),
        "shared_columns": sorted(columns1 & columns2),
        "common_columns_overlap": round(_jaccard(columns1, columns2), 4),
        "shared_samples": sorted(samples1 & samples2),
        "common_samples_overlap": round(_jaccard(samples1, samples2), 4),
    }


def compare_datalinkingbase(
        link1: Dict[str, Any],
        link2: Dict[str, Any],
        relation_threshold: float = 0.0
) -> Optional[Dict[str, Any]]:
    keywords1 = _as_set(link1.get("common_keywords"))
    keywords2 = _as_set(link2.get("common_keywords"))
    shared_keywords = sorted(keywords1 & keywords2)
    keyword_overlap = _jaccard(keywords1, keywords2)

    shared_profile_details = _shared_profile_details(link1, link2)
    shared_profiles = [profile.get("name") or profile.get("id") for profile in shared_profile_details]
    shared_profile_score = len(shared_profile_details) / 2.0

    metric_profile_similarity = _metric_profile_similarity(link1, link2)

    chunk_texts1 = _chunk_texts(link1)
    chunk_texts2 = _chunk_texts(link2)
    description_chunk_similarity = _jaccard(chunk_texts1, chunk_texts2)
    has_chunks = bool(chunk_texts1 or chunk_texts2)
    refinement_overlap = _refinement_overlap(link1, link2)

    if has_chunks:
        relation_similarity = (
            0.45 * keyword_overlap
            + 0.35 * metric_profile_similarity
            + 0.15 * description_chunk_similarity
            + 0.05 * shared_profile_score
        )
    else:
        relation_similarity = (
            0.55 * keyword_overlap
            + 0.40 * metric_profile_similarity
            + 0.05 * shared_profile_score
        )

    relation_similarity = round(relation_similarity, 4)
    if relation_similarity < relation_threshold:
        return None

    evidence = {
        "shared_profiles": shared_profiles,
        "shared_profile_details": shared_profile_details,
        "shared_profile_score": round(shared_profile_score, 4),
        "shared_keywords": shared_keywords,
        "keyword_overlap": round(keyword_overlap, 4),
        "metric_profile_similarity": round(metric_profile_similarity, 4),
        "description_chunk_similarity": round(description_chunk_similarity, 4),
    }
    if refinement_overlap:
        evidence["refinement_overlap"] = refinement_overlap

    return {
        "@type": "DataLinkingBaseComparison",
        "@id": f"comparison:{uuid.uuid4()}",
        "link1ref": link1.get("@id"),
        "link2ref": link2.get("@id"),
        "link1Label": _link_label(link1),
        "link2Label": _link_label(link2),
        "link1": _link_summary(link1),
        "link2": _link_summary(link2),
        "relation_similarity": relation_similarity,
        "evidence": evidence,
    }


def compare_datalinkingbase_links(
        links: List[Dict[str, Any]],
        relation_threshold: float = 0.0,
        top_n: Optional[int] = None
) -> List[Dict[str, Any]]:
    comparisons = []
    selected_links = links[:top_n] if top_n and top_n > 0 else links

    for link1, link2 in combinations(selected_links, 2):
        comparison = compare_datalinkingbase(link1, link2, relation_threshold)
        if comparison:
            comparisons.append(comparison)

    return sorted(
        comparisons,
        key=lambda item: item.get("relation_similarity", 0),
        reverse=True,
    )
