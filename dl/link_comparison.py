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


def _display_map(values: Any) -> Dict[str, str]:
    if not values:
        return {}
    if isinstance(values, str):
        values = [v.strip() for v in values.split(",")]

    mapped = {}
    for value in values:
        display = str(value).strip()
        if display:
            mapped.setdefault(display.lower(), display)
    return mapped


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


def _shared_profile_keys(link1: Dict[str, Any], link2: Dict[str, Any]) -> Set[str]:
    profiles1 = {_profile_key(profile) for profile in _profile_entries(link1) if _profile_key(profile)}
    profiles2 = {_profile_key(profile) for profile in _profile_entries(link2) if _profile_key(profile)}
    return profiles1 & profiles2


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


def _chunk_evidence_texts(link: Dict[str, Any]) -> List[str]:
    texts = []
    seen = set()
    for item in link.get("description_top_chunks", []) or []:
        if not isinstance(item, dict):
            continue
        chunks = [str(item.get("chunk1") or "").strip(), str(item.get("chunk2") or "").strip()]
        text = " | ".join(chunk for chunk in chunks if chunk)
        key = text.lower()
        if text and key not in seen:
            texts.append(text)
            seen.add(key)
    return texts


def _semantic_chunk_similarity(link1: Dict[str, Any], link2: Dict[str, Any], limit: int = 3) -> Dict[str, Any]:
    texts1 = _chunk_evidence_texts(link1)
    texts2 = _chunk_evidence_texts(link2)
    if not texts1 or not texts2:
        return {"similarity": 0.0, "matches": []}

    try:
        from sentence_transformers import util
        from dl import similarity

        similarity._ensure_models()
        if similarity._model_long is None:
            return {"similarity": 0.0, "matches": []}
        emb1 = similarity._model_long.encode(texts1, convert_to_tensor=True)
        emb2 = similarity._model_long.encode(texts2, convert_to_tensor=True)
        scores = util.cos_sim(emb1, emb2)
    except Exception:
        return {"similarity": 0.0, "matches": []}

    matches = []
    for idx, text in enumerate(texts1):
        best_idx = scores[idx].argmax().item()
        score = max(0.0, min(1.0, float(scores[idx][best_idx].item())))
        matches.append({
            "chunk_evidence_1": text,
            "chunk_evidence_2": texts2[best_idx],
            "similarity": round(score, 4),
        })

    matches = sorted(matches, key=lambda item: item["similarity"], reverse=True)[:limit]
    for rank, match in enumerate(matches, 1):
        match["rank"] = rank

    return {
        "similarity": round(_average([match["similarity"] for match in matches]), 4),
        "matches": matches,
    }


def _profile_evidence_for_entry(link: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
    evidence = link.get("profile_evidence", {}) or {}
    return evidence.get(profile.get("id")) or evidence.get(profile.get("name")) or {}


def _non_shared_profile_values(
        link: Dict[str, Any],
        shared_keys: Set[str],
        key: str,
        fallback_key: str,
) -> Dict[str, str]:
    values = []
    for profile in _profile_entries(link):
        if _profile_key(profile) in shared_keys:
            continue
        evidence = _profile_evidence_for_entry(link, profile)
        values.extend(evidence.get(key) or evidence.get(fallback_key) or [])
    return _display_map(values)


def _shared_display_values(values1: Dict[str, str], values2: Dict[str, str]) -> List[str]:
    return sorted(values1[key] for key in set(values1) & set(values2))


def _refinement_overlap(link1: Dict[str, Any], link2: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    shared_keys = _shared_profile_keys(link1, link2)
    files1_display = _non_shared_profile_values(link1, shared_keys, "resource_names", "files")
    files2_display = _non_shared_profile_values(link2, shared_keys, "resource_names", "files")
    columns1_display = _non_shared_profile_values(link1, shared_keys, "column_names", "columns")
    columns2_display = _non_shared_profile_values(link2, shared_keys, "column_names", "columns")

    files1 = set(files1_display)
    files2 = set(files2_display)
    columns1 = set(columns1_display)
    columns2 = set(columns2_display)

    if not any([files1, files2, columns1, columns2]):
        return None

    return {
        "shared_file_table_names": _shared_display_values(files1_display, files2_display),
        "file_table_name_overlap": round(_jaccard(files1, files2), 4),
        "shared_csv_table_column_names": _shared_display_values(columns1_display, columns2_display),
        "csv_table_column_name_overlap": round(_jaccard(columns1, columns2), 4),
    }


def _refinement_overlap_score(refinement_overlap: Optional[Dict[str, Any]]) -> float:
    if not refinement_overlap:
        return 0.0
    return round(max(
        float(refinement_overlap.get("file_table_name_overlap") or 0.0),
        float(refinement_overlap.get("csv_table_column_name_overlap") or 0.0),
    ), 4)


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

    chunk_evidence = _semantic_chunk_similarity(link1, link2)
    description_chunk_similarity = chunk_evidence["similarity"]
    has_chunks = bool(chunk_evidence["matches"])
    refinement_overlap = _refinement_overlap(link1, link2)
    refinement_score = _refinement_overlap_score(refinement_overlap)

    if has_chunks:
        relation_similarity = (
            0.70 * description_chunk_similarity
            + 0.25 * keyword_overlap
            + 0.05 * shared_profile_score
        )
    else:
        relation_similarity = (
            0.65 * keyword_overlap
            + 0.30 * refinement_score
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
        "description_chunk_matches": chunk_evidence["matches"],
        "refinement_overlap_score": refinement_score,
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


def _comparison_link_refs(comparison: Dict[str, Any]) -> Set[str]:
    return {comparison.get("link1ref"), comparison.get("link2ref")} - {None, ""}


def _profile_display_map(links: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    profiles = {}
    for link in links:
        for profile in _profile_entries(link):
            key = _profile_key(profile)
            if key:
                profiles.setdefault(key, {
                    "id": profile.get("id"),
                    "name": profile.get("name") or profile.get("id"),
                })
    return profiles


def _average(values: List[float]) -> float:
    return round(sum(values) / len(values), 4) if values else 0.0


def _composite_evidence(links: List[Dict[str, Any]], comparisons: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = set()
    file_table_names = {}
    csv_table_column_names = {}

    for link in links:
        keywords.update(_as_set(link.get("common_keywords")))
        for evidence in (link.get("profile_evidence", {}) or {}).values():
            file_table_names.update(_display_map(evidence.get("resource_names") or evidence.get("files")))
            csv_table_column_names.update(_display_map(evidence.get("column_names") or evidence.get("columns")))

    relation_similarities = [
        float(comparison.get("relation_similarity"))
        for comparison in comparisons
        if comparison.get("relation_similarity") is not None
    ]
    chunk_similarities = [
        float((comparison.get("evidence") or {}).get("description_chunk_similarity"))
        for comparison in comparisons
        if (comparison.get("evidence") or {}).get("description_chunk_similarity") is not None
    ]
    base_similarities = []
    for link in links:
        try:
            base_similarities.append(float((link.get("metrics") or {}).get("combined_similarity", 0)) / 100.0)
        except (TypeError, ValueError):
            continue

    return {
        "shared_keywords": sorted(keywords),
        "file_table_names": sorted(file_table_names.values()),
        "csv_table_column_names": sorted(csv_table_column_names.values()),
        "base_link_similarity_avg": _average(base_similarities),
        "relation_similarity_avg": _average(relation_similarities),
        "relation_similarity_min": round(min(relation_similarities), 4) if relation_similarities else 0.0,
        "relation_similarity_max": round(max(relation_similarities), 4) if relation_similarities else 0.0,
        "chunk_similarity_avg": _average(chunk_similarities),
    }


def _link_level_item(link: Dict[str, Any]) -> Dict[str, Any]:
    metrics = link.get("metrics") or {}
    try:
        similarity = float(metrics.get("combined_similarity", 0)) / 100.0
    except (TypeError, ValueError):
        similarity = 0.0
    return {
        "@type": "HierarchyLevelItem",
        "item_type": "DataLinkingBase",
        "id": link.get("@id"),
        "label": _link_label(link),
        "similarity": round(similarity, 4),
        "linkRefs": [link.get("@id")] if link.get("@id") else [],
        "originRefs": [link.get("@id")] if link.get("@id") else [],
        "recursive_level": 1,
        "representative_chunks": _representative_chunks_from_link(link),
        "profiles": _profile_entries(link),
        "evidence": {
            "shared_keywords": sorted(_as_set(link.get("common_keywords"))),
            "description_top_chunks": link.get("description_top_chunks", []) or [],
        },
    }


def _comparison_level_item(comparison: Dict[str, Any]) -> Dict[str, Any]:
    evidence = comparison.get("evidence") or {}
    return {
        "@type": "HierarchyLevelItem",
        "item_type": "DataLinkingBaseComparison",
        "id": comparison.get("@id"),
        "label": f"{comparison.get('link1Label', '')} <-> {comparison.get('link2Label', '')}",
        "similarity": comparison.get("relation_similarity", 0.0),
        "sourceRefs": sorted(_comparison_link_refs(comparison)),
        "evidence": {
            "shared_profiles": evidence.get("shared_profiles", []),
            "shared_keywords": evidence.get("shared_keywords", []),
            "description_chunk_similarity": evidence.get("description_chunk_similarity", 0.0),
            "description_chunk_matches": evidence.get("description_chunk_matches", []),
            "refinement_overlap": evidence.get("refinement_overlap"),
        },
    }


def _representative_chunks_from_link(link: Dict[str, Any], limit: int = 3) -> List[Dict[str, Any]]:
    chunks = []
    for item in link.get("description_top_chunks", []) or []:
        if not isinstance(item, dict):
            continue
        chunk1 = str(item.get("chunk1") or "").strip()
        chunk2 = str(item.get("chunk2") or "").strip()
        text = " | ".join(chunk for chunk in [chunk1, chunk2] if chunk)
        if not text:
            continue
        try:
            similarity = float(item.get("similarity") or 0.0) / 100.0
        except (TypeError, ValueError):
            similarity = 0.0
        chunks.append({
            "text": text,
            "source_similarity": round(similarity, 4),
            "source": link.get("@id"),
        })
    return chunks[:limit]


def _representative_chunks_from_matches(matches: List[Dict[str, Any]], limit: int = 3) -> List[Dict[str, Any]]:
    chunks = []
    seen = set()
    for match in matches[:limit]:
        text1 = str(match.get("chunk_evidence_1") or "").strip()
        text2 = str(match.get("chunk_evidence_2") or "").strip()
        text = " | ".join(text for text in [text1, text2] if text)
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        chunks.append({
            "text": text,
            "source_similarity": round(float(match.get("similarity") or 0.0), 4),
        })
    return chunks


def _item_representative_chunks(item: Dict[str, Any], limit: int = 3) -> List[Dict[str, Any]]:
    return [
        chunk
        for chunk in item.get("representative_chunks", [])[:limit]
        if str(chunk.get("text") or "").strip()
    ]


def _compare_representative_chunks(
        left: Dict[str, Any],
        right: Dict[str, Any],
        limit: int = 3,
) -> Dict[str, Any]:
    left_chunks = _item_representative_chunks(left, limit)
    right_chunks = _item_representative_chunks(right, limit)
    if not left_chunks or not right_chunks:
        return {"similarity": 0.0, "matches": []}

    try:
        from sentence_transformers import util
        from dl import similarity

        similarity._ensure_models()
        if similarity._model_long is None:
            return {"similarity": 0.0, "matches": []}

        left_texts = [chunk["text"] for chunk in left_chunks]
        right_texts = [chunk["text"] for chunk in right_chunks]
        emb1 = similarity._model_long.encode(left_texts, convert_to_tensor=True)
        emb2 = similarity._model_long.encode(right_texts, convert_to_tensor=True)
        scores = util.cos_sim(emb1, emb2)
    except Exception:
        return {"similarity": 0.0, "matches": []}

    matches = []
    used_pairs = set()
    for left_idx, left_chunk in enumerate(left_chunks):
        for right_idx, right_chunk in enumerate(right_chunks):
            pair_key = (left_idx, right_idx)
            if pair_key in used_pairs:
                continue
            used_pairs.add(pair_key)
            score = max(0.0, min(1.0, float(scores[left_idx][right_idx].item())))
            matches.append({
                "chunk_evidence_1": left_chunk["text"],
                "chunk_evidence_2": right_chunk["text"],
                "similarity": round(score, 4),
            })

    matches = sorted(matches, key=lambda item: item["similarity"], reverse=True)[:limit]
    for rank, match in enumerate(matches, 1):
        match["rank"] = rank

    return {
        "similarity": _average([match["similarity"] for match in matches]),
        "matches": matches,
    }


def _item_link_refs(item: Dict[str, Any]) -> Set[str]:
    return {str(ref) for ref in item.get("linkRefs", []) if ref}


def _item_profile_keys(item: Dict[str, Any]) -> Set[str]:
    return {_profile_key(profile) for profile in item.get("profiles", []) or [] if _profile_key(profile)}


def _item_origin_refs(item: Dict[str, Any]) -> List[str]:
    refs = item.get("originRefs") or item.get("linkRefs") or []
    return sorted(str(ref) for ref in refs if ref)


def _item_pair_key(left: Dict[str, Any], right: Dict[str, Any]) -> tuple:
    return tuple(sorted([
        str(left.get("id") or "|".join(_item_origin_refs(left))),
        str(right.get("id") or "|".join(_item_origin_refs(right))),
    ]))


def _bridge_comparisons(
        left_refs: Set[str],
        right_refs: Set[str],
        comparisons: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    union_refs = left_refs | right_refs
    return [
        comparison
        for comparison in comparisons
        if _comparison_link_refs(comparison) <= union_refs
        and _comparison_link_refs(comparison) & left_refs
        and _comparison_link_refs(comparison) & right_refs
    ]


def _compare_level_items(
        left: Dict[str, Any],
        right: Dict[str, Any],
        comparisons: List[Dict[str, Any]],
        level_number: int,
        relation_threshold: float,
) -> Optional[Dict[str, Any]]:
    left_refs = _item_link_refs(left)
    right_refs = _item_link_refs(right)
    merged_refs = left_refs | right_refs
    if not left_refs or not right_refs or merged_refs == left_refs or merged_refs == right_refs:
        return None

    bridges = _bridge_comparisons(left_refs, right_refs, comparisons)
    chunk_comparison = _compare_representative_chunks(left, right)
    chunk_similarity = chunk_comparison["similarity"]
    if chunk_similarity < relation_threshold:
        return None

    shared_keywords = set()
    shared_profiles = set()
    for comparison in bridges:
        evidence = comparison.get("evidence") or {}
        shared_keywords.update(evidence.get("shared_keywords", []) or [])
        shared_profiles.update(evidence.get("shared_profiles", []) or [])

    return {
        "@type": "RecursiveCompositeComparison",
        "@id": f"recursive-comparison:{uuid.uuid4()}",
        "recursive_level": level_number,
        "leftRef": left.get("id"),
        "rightRef": right.get("id"),
        "leftOriginRefs": _item_origin_refs(left),
        "rightOriginRefs": _item_origin_refs(right),
        "originLinkRefs": sorted(merged_refs),
        "bridgeComparisonRefs": [comparison.get("@id") for comparison in bridges if comparison.get("@id")],
        "relation_similarity": chunk_similarity,
        "chunk_similarity": chunk_similarity,
        "evidence": {
            "shared_profiles": sorted(shared_profiles),
            "shared_keywords": sorted(shared_keywords),
            "description_chunk_matches": chunk_comparison["matches"],
        },
    }


def _composite_level_item(
        level_number: int,
        link_ids: Set[str],
        links_by_id: Dict[str, Dict[str, Any]],
        comparisons: List[Dict[str, Any]],
        parent_items: List[Dict[str, Any]],
        recursive_comparison: Dict[str, Any],
) -> Dict[str, Any]:
    links = [links_by_id[link_id] for link_id in sorted(link_ids)]
    component_comparisons = [comparison for comparison in comparisons if _comparison_link_refs(comparison) <= link_ids]
    comparison_ids = [comparison.get("@id") for comparison in component_comparisons]
    profile_map = _profile_display_map(links)
    profile_keys = set()
    for link in links:
        profile_keys.update(_profile_key(profile) for profile in _profile_entries(link) if _profile_key(profile))
    evidence = _composite_evidence(links, component_comparisons)
    recursive_evidence = recursive_comparison.get("evidence") or {}
    bridge_similarity = recursive_comparison.get("relation_similarity", 0.0)
    evidence["recursive_chunk_similarity"] = recursive_comparison.get("chunk_similarity", bridge_similarity)
    evidence["chunk_similarity_avg"] = evidence["recursive_chunk_similarity"]
    return {
        "@type": "HierarchyLevelItem",
        "item_type": "CompositeDataLinkingElement",
        "id": f"hierarchy:level:{level_number}:{uuid.uuid4()}",
        "label": f"Composite of {len(links)} DataLinkingBase link(s)",
        "similarity": bridge_similarity or evidence["relation_similarity_avg"],
        "linkRefs": sorted(link_ids),
        "originRefs": sorted(link_ids),
        "comparisonRefs": sorted(item for item in comparison_ids if item),
        "parentRefs": [item.get("id") for item in parent_items if item.get("id")],
        "recursiveComparisonRef": recursive_comparison.get("@id"),
        "supportingComparisons": [recursive_comparison],
        "recursive_level": level_number,
        "representative_chunks": _representative_chunks_from_matches(
            recursive_evidence.get("description_chunk_matches", [])
        ),
        "profiles": [profile_map[key] for key in sorted(profile_keys) if key in profile_map],
        "mergeEvidence": {
            "recursiveComparisonRef": recursive_comparison.get("@id"),
            "bridgeComparisonRefs": recursive_comparison.get("bridgeComparisonRefs", []),
            "relation_similarity": bridge_similarity,
            "chunk_similarity": recursive_comparison.get("chunk_similarity", bridge_similarity),
            "description_chunk_matches": recursive_evidence.get("description_chunk_matches", []),
            "shared_keywords": recursive_evidence.get("shared_keywords", []),
        },
        "evidence": evidence,
    }


def _merge_duplicate_composite_item(
        item: Dict[str, Any],
        recursive_comparison: Dict[str, Any],
        links_by_id: Dict[str, Dict[str, Any]],
        comparisons: List[Dict[str, Any]],
) -> None:
    supporting = item.setdefault("supportingComparisons", [])
    supporting.append(recursive_comparison)

    similarities = [
        float(comparison.get("chunk_similarity") or comparison.get("relation_similarity") or 0.0)
        for comparison in supporting
    ]
    item["similarity"] = _average(similarities)
    item["recursiveComparisonRef"] = recursive_comparison.get("@id")
    link_refs = set(item.get("linkRefs", [])) | set(recursive_comparison.get("originLinkRefs", []))
    item["linkRefs"] = sorted(link_refs)
    item["originRefs"] = sorted(link_refs)

    valid_links = [links_by_id[link_id] for link_id in sorted(link_refs) if link_id in links_by_id]
    component_comparisons = [comparison for comparison in comparisons if _comparison_link_refs(comparison) <= link_refs]
    comparison_ids = [comparison.get("@id") for comparison in component_comparisons if comparison.get("@id")]
    profile_map = _profile_display_map(valid_links)
    profile_keys = set()
    for link in valid_links:
        profile_keys.update(_profile_key(profile) for profile in _profile_entries(link) if _profile_key(profile))
    item["comparisonRefs"] = sorted(comparison_ids)
    item["profiles"] = [profile_map[key] for key in sorted(profile_keys) if key in profile_map]

    all_matches = []
    all_keywords = set()
    bridge_refs = []
    for comparison in supporting:
        evidence = comparison.get("evidence") or {}
        all_matches.extend(evidence.get("description_chunk_matches", []) or [])
        all_keywords.update(evidence.get("shared_keywords", []) or [])
        bridge_refs.extend(comparison.get("bridgeComparisonRefs", []) or [])

    top_matches = sorted(
        all_matches,
        key=lambda match: match.get("similarity", 0.0),
        reverse=True,
    )[:3]
    for rank, match in enumerate(top_matches, 1):
        match["rank"] = rank

    item["representative_chunks"] = _representative_chunks_from_matches(top_matches)
    item["mergeEvidence"] = {
        "supportingComparisonRefs": [comparison.get("@id") for comparison in supporting if comparison.get("@id")],
        "bridgeComparisonRefs": sorted(set(ref for ref in bridge_refs if ref)),
        "relation_similarity": item["similarity"],
        "chunk_similarity": item["similarity"],
        "description_chunk_matches": top_matches,
        "shared_keywords": sorted(all_keywords),
    }
    evidence = _composite_evidence(valid_links, component_comparisons)
    evidence["recursive_chunk_similarity"] = item["similarity"]
    evidence["chunk_similarity_avg"] = item["similarity"]
    item["evidence"] = evidence


def _build_hierarchy_levels(
        links: List[Dict[str, Any]],
        comparisons: List[Dict[str, Any]],
        relation_threshold: float,
) -> tuple:
    links_by_id = {link.get("@id"): link for link in links if link.get("@id")}
    base_items = [_link_level_item(link) for link in links]
    candidate_items = list(base_items)
    levels = [
        {
            "level": 1,
            "name": "Profile-to-profile links",
            "description": "Initial DataLinkingBase links between dataset profiles.",
            "items": base_items,
        },
    ]

    seen_groups = {frozenset(_item_profile_keys(item)) for item in candidate_items if _item_profile_keys(item)}
    recursive_comparisons = []
    current_level = 2

    while len(candidate_items) > 1:
        next_items = []
        next_groups = {}
        compared_pairs = set()
        for left, right in combinations(candidate_items, 2):
            pair_key = _item_pair_key(left, right)
            if pair_key in compared_pairs:
                continue
            compared_pairs.add(pair_key)

            left_refs = _item_link_refs(left)
            right_refs = _item_link_refs(right)
            merged_refs = left_refs | right_refs
            left_profiles = _item_profile_keys(left)
            right_profiles = _item_profile_keys(right)
            merged_profiles = left_profiles | right_profiles
            group_key = frozenset(merged_profiles)

            if not left_refs or not right_refs or not merged_profiles:
                continue
            if merged_profiles == left_profiles or merged_profiles == right_profiles:
                continue
            if group_key in seen_groups:
                continue

            recursive_comparison = _compare_level_items(
                left,
                right,
                comparisons,
                current_level,
                relation_threshold,
            )
            if not recursive_comparison:
                continue

            if group_key in next_groups:
                _merge_duplicate_composite_item(next_groups[group_key], recursive_comparison, links_by_id, comparisons)
                recursive_comparisons.append(recursive_comparison)
                continue

            next_item = _composite_level_item(
                current_level,
                merged_refs,
                links_by_id,
                comparisons,
                [left, right],
                recursive_comparison,
            )
            next_items.append(next_item)
            next_groups[group_key] = next_item
            recursive_comparisons.append(recursive_comparison)

        if not next_items:
            break

        next_items.sort(
            key=lambda item: (len(item.get("linkRefs", [])), item.get("similarity", 0.0)),
            reverse=True,
        )
        levels.append({
            "level": current_level,
            "name": f"Composite merge level {current_level - 1}",
            "description": "CompositeDataLinkingElement items produced by comparing every pair from the previous level.",
            "items": next_items,
        })
        seen_groups.update(next_groups)
        candidate_items.extend(next_items)
        current_level += 1

    return levels, recursive_comparisons


def _composite_from_level_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "@type": "CompositeDataLinkingElement",
        "@id": item.get("id"),
        "profiles": item.get("profiles", []),
        "linkRefs": item.get("linkRefs", []),
        "comparisonRefs": item.get("comparisonRefs", []),
        "parentRefs": item.get("parentRefs", []),
        "link_count": len(item.get("linkRefs", [])),
        "profile_count": len(item.get("profiles", [])),
        "comparison_count": len(item.get("comparisonRefs", [])),
        "composite_coherence": item.get("similarity", 0.0),
        "recursive_level": item.get("recursive_level"),
        "recursiveComparisonRef": item.get("recursiveComparisonRef"),
        "supportingComparisons": item.get("supportingComparisons", []),
        "representative_chunks": item.get("representative_chunks", []),
        "evidence": item.get("evidence", {}),
        "mergeEvidence": item.get("mergeEvidence", {}),
    }


def _terminal_composite_items(levels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    composite_items = [
        item
        for level in levels[1:]
        for item in level.get("items", [])
        if item.get("item_type") == "CompositeDataLinkingElement"
    ]
    used_as_parent = {
        parent_ref
        for item in composite_items
        for parent_ref in item.get("parentRefs", [])
        if parent_ref
    }
    return [
        item
        for item in composite_items
        if item.get("id") not in used_as_parent
    ]


def _component_composite(
        component_link_ids: Set[str],
        links_by_id: Dict[str, Dict[str, Any]],
        comparisons: List[Dict[str, Any]],
) -> Dict[str, Any]:
    component_links = [links_by_id[item] for item in sorted(component_link_ids)]
    component_comparisons = [
        comparison
        for comparison in comparisons
        if _comparison_link_refs(comparison) <= component_link_ids
    ]
    profile_map = _profile_display_map(component_links)
    profile_keys = set()
    for link in component_links:
        profile_keys.update(_profile_key(profile) for profile in _profile_entries(link) if _profile_key(profile))

    evidence = _composite_evidence(component_links, component_comparisons)
    return {
        "@type": "CompositeDataLinkingElement",
        "@id": f"composite:{uuid.uuid4()}",
        "profiles": [profile_map[key] for key in sorted(profile_keys) if key in profile_map],
        "linkRefs": sorted(component_link_ids),
        "comparisonRefs": sorted(comparison.get("@id") for comparison in component_comparisons if comparison.get("@id")),
        "link_count": len(component_links),
        "profile_count": len(profile_keys),
        "comparison_count": len(component_comparisons),
        "composite_coherence": evidence["relation_similarity_avg"],
        "evidence": evidence,
        "construction": "connected_component",
    }


def _build_component_composites(
        links: List[Dict[str, Any]],
        comparisons: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    links_by_id = {link.get("@id"): link for link in links if link.get("@id")}
    adjacency = {link_id: set() for link_id in links_by_id}

    for comparison in comparisons:
        refs = list(_comparison_link_refs(comparison))
        if len(refs) != 2:
            continue
        left, right = refs
        if left in adjacency and right in adjacency:
            adjacency[left].add(right)
            adjacency[right].add(left)

    visited = set()
    components = []
    for link_id in sorted(adjacency):
        if link_id in visited or not adjacency[link_id]:
            continue

        stack = [link_id]
        component_link_ids = set()
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            component_link_ids.add(current)
            stack.extend(sorted(adjacency[current] - visited))

        components.append(_component_composite(component_link_ids, links_by_id, comparisons))

    components.sort(
        key=lambda item: (item.get("profile_count", 0), item.get("composite_coherence", 0)),
        reverse=True,
    )
    return components


def _component_level_item(composite: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "@type": "HierarchyLevelItem",
        "item_type": "CompositeDataLinkingElement",
        "id": composite.get("@id"),
        "label": f"Cluster of {composite.get('profile_count', 0)} profile(s) from {composite.get('link_count', 0)} link(s)",
        "similarity": composite.get("composite_coherence", 0.0),
        "linkRefs": composite.get("linkRefs", []),
        "comparisonRefs": composite.get("comparisonRefs", []),
        "profiles": composite.get("profiles", []),
        "evidence": composite.get("evidence", {}),
    }


def _build_component_levels(
        links: List[Dict[str, Any]],
        components: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return [
        {
            "level": 1,
            "name": "Profile-to-profile links",
            "description": "Initial DataLinkingBase links between dataset profiles above the selected threshold.",
            "items": [_link_level_item(link) for link in links],
        },
        {
            "level": 2,
            "name": "Connected composite clusters",
            "description": "Final CompositeDataLinkingElement groups built from connected links. This is the main, interpretable output.",
            "items": [_component_level_item(component) for component in components],
        },
    ]


def build_composite_datalinking_elements(
        links: List[Dict[str, Any]],
        relation_threshold: float = 0.5,
        top_n: Optional[int] = None,
) -> Dict[str, Any]:
    selected_links = links[:top_n] if top_n and top_n > 0 else links
    comparisons = compare_datalinkingbase_links(
        selected_links,
        relation_threshold=relation_threshold,
    )
    recursive_levels, recursive_comparisons = _build_hierarchy_levels(selected_links, comparisons, relation_threshold)
    recursive_components = [_composite_from_level_item(item) for item in _terminal_composite_items(recursive_levels)]
    component_composites = _build_component_composites(selected_links, comparisons)
    component_levels = _build_component_levels(selected_links, component_composites)
    if recursive_components:
        components = recursive_components
        levels = recursive_levels
        construction = "recursive_chunk_similarity"
    else:
        components = component_composites
        levels = component_levels
        construction = "connected_component_fallback"
    components.sort(
        key=lambda item: (item.get("link_count", 0), item.get("composite_coherence", 0)),
        reverse=True,
    )
    return {
        "comparisons": comparisons,
        "recursive_comparisons": recursive_comparisons,
        "recursive_levels": recursive_levels,
        "connected_component_composites": component_composites,
        "connected_component_levels": component_levels,
        "construction": construction,
        "composites": components,
        "levels": levels,
    }
