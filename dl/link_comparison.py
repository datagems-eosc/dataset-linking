import math
import uuid
from itertools import combinations, product
from typing import Any, Dict, List, Optional, Set


METRIC_KEYS = [
    "keywords_similarity",
    "description_similarity",
    "headline_similarity",
    "combined_similarity",
]

# Recursive hierarchy scoring. Chunks remain the main semantic signal, while
# keywords contribute explicitly at every merge level.
RECURSIVE_CHUNK_WEIGHT = 0.75
RECURSIVE_KEYWORD_WEIGHT = 0.25
REPRESENTATIVE_CHUNK_LIMIT = 3
MAX_PROFILE_CHUNK_CANDIDATES = 10
MAX_EXHAUSTIVE_CHUNK_COMBINATIONS = 10000
MAX_BEAM_PROFILE_COUNT = 20
MAX_RECURSIVE_BASE_LINKS = 1000
MAX_SHARED_GROUP_TEXT_OVERLAP = 0.75
CHUNK_BEAM_WIDTH = 200

# Legacy latent-bridge configuration is retained for backward-compatible
# function signatures; the production hierarchy now requires overlapping
# green parent groups.
LATENT_RELATION_THRESHOLD = 0.70
LATENT_PROFILE_PAIR_THRESHOLD = 0.65
LATENT_MIN_STRONG_PROFILE_PAIRS = 2
LATENT_MIN_PROFILE_COVERAGE = 0.50


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


def _profile_pair_key(left: str, right: str) -> tuple:
    return tuple(sorted((str(left).strip().lower(), str(right).strip().lower())))


def _multi_profile_chunk_evidence(
        profile_keys: List[str],
        profile_chunks: Dict[str, List[str]],
        profile_labels: Optional[Dict[str, str]] = None,
        limit: int = 3,
        cache: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Find one-chunk-per-profile evidence groups using cached pair scores."""
    cache = cache if cache is not None else {}
    selected_chunks = cache.setdefault("chunks", {})
    embeddings_cache = cache.setdefault("embeddings", {})
    pair_cache = cache.setdefault("pair_scores", {})

    for profile in sorted(profile_keys):
        if profile in selected_chunks:
            continue
        values = []
        seen = set()
        for value in profile_chunks.get(profile, []) or []:
            text = str(value or "").strip()
            normalized = " ".join(text.split()).lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            values.append(text)
        selected_chunks[profile] = sorted(
            values,
            key=lambda value: (-len(value), value.lower()),
        )[:MAX_PROFILE_CHUNK_CANDIDATES]

    profiles = [key for key in sorted(profile_keys) if selected_chunks.get(key)]
    if len(profiles) < 2:
        return []

    try:
        from sentence_transformers import util
        from dl import similarity

        similarity._ensure_models()
        if similarity._model_long is None:
            return []

        for profile in profiles:
            if profile not in embeddings_cache:
                embeddings_cache[profile] = similarity._model_long.encode(
                    selected_chunks[profile],
                    convert_to_tensor=True,
                )

        def pair_matrix(left, right):
            key = _profile_pair_key(left, right)
            if key not in pair_cache:
                pair_cache[key] = util.cos_sim(
                    embeddings_cache[key[0]],
                    embeddings_cache[key[1]],
                )
            matrix = pair_cache[key]
            return matrix if left == key[0] else matrix.T

        def evaluate(indexes):
            active_profiles = profiles[:len(indexes)]
            selected = {
                profile: indexes[position]
                for position, profile in enumerate(active_profiles)
            }
            pair_scores = []
            pair_details = []
            for left, right in combinations(active_profiles, 2):
                score = float(pair_matrix(left, right)[selected[left], selected[right]].item())
                score = max(0.0, min(1.0, score))
                pair_scores.append(score)
                pair_details.append({
                    "pair": [left, right],
                    "similarity": round(score, 4),
                })
            minimum = min(pair_scores) if pair_scores else 1.0
            average = sum(pair_scores) / len(pair_scores) if pair_scores else 1.0
            return {
                "similarity": round(0.5 * minimum + 0.5 * average, 4),
                "minimum_pair_similarity": round(minimum, 4),
                "average_pair_similarity": round(average, 4),
                "indexes": selected,
                "pairwise_scores": pair_details,
            }

        combination_count = 1
        for profile in profiles:
            combination_count *= len(selected_chunks[profile])

        candidates = []
        if len(profiles) > MAX_BEAM_PROFILE_COUNT:
            # For very large composites, a full beam search repeatedly scores
            # every profile pair and becomes slower than the build itself.
            # Choose one chunk per profile against the global semantic centroid
            # and evaluate that single all-profile candidate.
            centroid = sum(
                embeddings_cache[profile].mean(dim=0, keepdim=True)
                for profile in profiles
            ) / len(profiles)
            indexes = [
                int(util.cos_sim(embeddings_cache[profile], centroid).argmax().item())
                for profile in profiles
            ]
            candidates = [evaluate(indexes)]
        elif combination_count <= MAX_EXHAUSTIVE_CHUNK_COMBINATIONS:
            candidates = [
                evaluate(indexes)
                for indexes in product(*(range(len(selected_chunks[profile])) for profile in profiles))
            ]
        else:
            beam = [([], 0.0)]
            for position, profile in enumerate(profiles):
                expanded = []
                for indexes, unused_score in beam:
                    for chunk_index in range(len(selected_chunks[profile])):
                        candidate_indexes = indexes + [chunk_index]
                        if len(candidate_indexes) == 1:
                            score = 1.0
                        else:
                            partial = evaluate(candidate_indexes)
                            score = partial["similarity"]
                        expanded.append((candidate_indexes, score))
                expanded.sort(key=lambda item: item[1], reverse=True)
                beam = expanded[:CHUNK_BEAM_WIDTH]
            candidates = [evaluate(indexes) for indexes, unused_score in beam]

        for candidate in candidates:
            candidate["profiles"] = []
            for profile in profiles:
                index = candidate["indexes"][profile]
                candidate["profiles"].append({
                    "profile": (profile_labels or {}).get(profile, profile),
                    "profile_id": profile,
                    "text": selected_chunks[profile][index],
                    "similarity": round(
                        min(
                            detail["similarity"]
                            for detail in candidate["pairwise_scores"]
                            if profile in detail["pair"]
                        ),
                        4,
                    ),
                })
            candidate.pop("indexes", None)

        candidates.sort(key=lambda item: item["similarity"], reverse=True)
        unique = []
        for candidate in candidates:
            candidate_texts = {
                item["profile_id"]: _normalized_text(item["text"])
                for item in candidate["profiles"]
            }
            too_similar = False
            for previous in unique:
                previous_texts = {
                    item["profile_id"]: _normalized_text(item["text"])
                    for item in previous["profiles"]
                }
                shared_profiles = set(candidate_texts) & set(previous_texts)
                overlap = sum(
                    candidate_texts[profile] == previous_texts[profile]
                    for profile in shared_profiles
                ) / len(shared_profiles) if shared_profiles else 0.0
                if overlap >= MAX_SHARED_GROUP_TEXT_OVERLAP:
                    too_similar = True
                    break
            if too_similar:
                continue
            unique.append(candidate)
            if len(unique) >= limit:
                break
        for rank, candidate in enumerate(unique, 1):
            candidate["rank"] = rank
        return unique
    except Exception:
        return []


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
        "status": "green" if relation_similarity >= relation_threshold else "red",
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


def _link_level_item(link: Dict[str, Any], threshold: float = 0.5) -> Dict[str, Any]:
    metrics = link.get("metrics") or {}
    try:
        similarity = float(metrics.get("combined_similarity", 0)) / 100.0
    except (TypeError, ValueError):
        similarity = 0.0

    keyword_evidence = _keyword_evidence_from_links([link])
    representative_keywords = sorted(
        item.get("keyword") or key
        for key, item in keyword_evidence.items()
    )
    return {
        "@type": "HierarchyLevelItem",
        "item_type": "DataLinkingBase",
        "id": link.get("@id"),
        "label": _link_label(link),
        "similarity": round(similarity, 4),
        "status": "green" if similarity >= threshold else "red",
        "base_link_similarity": round(similarity, 4),
        "linkRefs": [link.get("@id")] if link.get("@id") else [],
        "originRefs": [link.get("@id")] if link.get("@id") else [],
        "recursive_level": 1,
        "representative_chunks": _representative_chunks_from_link(link),
        "representative_keywords": representative_keywords,
        "keyword_evidence": keyword_evidence,
        "profiles": _profile_entries(link),
        "evidence": {
            "shared_keywords": representative_keywords,
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
        "status": comparison.get("status", "green"),
        "sourceRefs": sorted(_comparison_link_refs(comparison)),
        "evidence": {
            "shared_profiles": evidence.get("shared_profiles", []),
            "shared_keywords": evidence.get("shared_keywords", []),
            "description_chunk_similarity": evidence.get("description_chunk_similarity", 0.0),
            "description_chunk_matches": evidence.get("description_chunk_matches", []),
            "refinement_overlap": evidence.get("refinement_overlap"),
        },
    }


def _dedupe_texts(values: List[str]) -> List[str]:
    result = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        key = text.lower()
        if text and key not in seen:
            seen.add(key)
            result.append(text)
    return result



def _normalized_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _evidence_profile_key(entry: Dict[str, Any]) -> str:
    return str(
        entry.get("profile_key")
        or entry.get("profile_id")
        or entry.get("profile_name")
        or ""
    ).strip().lower()


def _dedupe_evidence(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result = []
    seen = set()
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        value = str(entry.get("text") or "").strip()
        if not value:
            continue
        profile_key = _evidence_profile_key(entry)
        key = (profile_key, _normalized_text(value))
        if key in seen:
            continue
        seen.add(key)
        clean = {
            "text": value,
            "profile_id": str(entry.get("profile_id") or "").strip(),
            "profile_name": str(entry.get("profile_name") or "").strip(),
            "profile_key": profile_key,
        }
        source_link = str(entry.get("source_link") or entry.get("source") or "").strip()
        if source_link:
            clean["source_link"] = source_link
        result.append(clean)
    return result


def _chunk_evidence_entries(chunk: Dict[str, Any]) -> List[Dict[str, Any]]:
    evidence = chunk.get("evidence")
    if isinstance(evidence, list):
        cleaned = _dedupe_evidence(evidence)
        if cleaned:
            return cleaned

    # Backward-compatible fallback for older serialized representative chunks.
    texts = chunk.get("texts")
    if isinstance(texts, list) and texts:
        return _dedupe_evidence([{"text": value} for value in texts])
    text = str(chunk.get("text") or "").strip()
    return _dedupe_evidence([{"text": text}]) if text else []


def _keyword_evidence_from_links(links: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    evidence: Dict[str, Dict[str, Any]] = {}
    for link in links:
        profiles = _profile_entries(link)
        profile_support = {
            _profile_key(profile): {
                "id": profile.get("id") or "",
                "name": profile.get("name") or profile.get("id") or "",
            }
            for profile in profiles
            if _profile_key(profile)
        }
        for display in _display_map(link.get("common_keywords")).values():
            key = display.lower()
            item = evidence.setdefault(key, {
                "keyword": display,
                "profiles": {},
                "source_links": set(),
            })
            item["profiles"].update(profile_support)
            if link.get("@id"):
                item["source_links"].add(str(link.get("@id")))

    result = {}
    for key, item in evidence.items():
        result[key] = {
            "keyword": item["keyword"],
            "profiles": [item["profiles"][p] for p in sorted(item["profiles"])],
            "source_links": sorted(item["source_links"]),
        }
    return result


def _item_keyword_evidence(item: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    raw = item.get("keyword_evidence") or {}
    if isinstance(raw, dict) and raw:
        return raw

    # Backward-compatible fallback. Do not discard the keyword vocabulary.
    result = {}
    profiles = item.get("profiles", []) or []
    for display in sorted(_item_representative_keywords(item)):
        result[display.lower()] = {
            "keyword": display,
            "profiles": [
                {"id": p.get("id") or "", "name": p.get("name") or p.get("id") or ""}
                for p in profiles
            ],
            "source_links": _item_origin_refs(item),
        }
    return result


def _keyword_support_keys(keyword_item: Dict[str, Any]) -> Set[str]:
    return {
        _profile_key(profile)
        for profile in keyword_item.get("profiles", []) or []
        if _profile_key(profile)
    }


def _recursive_parent_coherences(parent_items: List[Dict[str, Any]]) -> List[float]:
    values = []
    for parent in parent_items:
        # Level-1 `similarity` is the legacy DataLinkingBase combined score and
        # is not on the same scale/definition as recursive merge coherence.
        if parent.get("item_type") != "CompositeDataLinkingElement":
            continue
        raw = parent.get("recursive_coherence")
        if raw is None:
            raw = parent.get("similarity")
        try:
            values.append(float(raw))
        except (TypeError, ValueError):
            continue
    return values

def _chunk_text_values(chunk: Dict[str, Any]) -> List[str]:
    evidence = _chunk_evidence_entries(chunk)
    if evidence:
        return _dedupe_texts([entry["text"] for entry in evidence])
    text = str(chunk.get("text") or "").strip()
    return [text] if text else []

def _representative_chunks_from_link(
        link: Dict[str, Any],
        limit: int = REPRESENTATIVE_CHUNK_LIMIT,
) -> List[Dict[str, Any]]:
    chunks = []
    profiles = _profile_entries(link)
    left_profile = profiles[0] if profiles else {}
    right_profile = profiles[1] if len(profiles) > 1 else {}

    for item in link.get("description_top_chunks", []) or []:
        if not isinstance(item, dict):
            continue

        evidence = []
        chunk1 = str(item.get("chunk1") or "").strip()
        chunk2 = str(item.get("chunk2") or "").strip()
        if chunk1:
            evidence.append({
                "text": chunk1,
                "profile_id": left_profile.get("id") or "",
                "profile_name": left_profile.get("name") or "",
                "profile_key": _profile_key(left_profile),
                "source_link": link.get("@id") or "",
            })
        if chunk2:
            evidence.append({
                "text": chunk2,
                "profile_id": right_profile.get("id") or "",
                "profile_name": right_profile.get("name") or "",
                "profile_key": _profile_key(right_profile),
                "source_link": link.get("@id") or "",
            })
        evidence = _dedupe_evidence(evidence)
        if not evidence:
            continue

        try:
            similarity = float(item.get("similarity") or 0.0) / 100.0
        except (TypeError, ValueError):
            similarity = 0.0

        texts = _dedupe_texts([entry["text"] for entry in evidence])
        chunks.append({
            "text": " | ".join(texts),
            "texts": texts,
            "evidence": evidence,
            "source_similarity": round(similarity, 4),
            "source": link.get("@id"),
        })
    return chunks[:limit]

def _representative_chunks_from_matches(
        matches: List[Dict[str, Any]],
        limit: int = REPRESENTATIVE_CHUNK_LIMIT,
) -> List[Dict[str, Any]]:
    chunks = []
    seen = set()
    for match in matches[:limit]:
        evidence = _dedupe_evidence(
            list(match.get("left_evidence") or [])
            + list(match.get("right_evidence") or [])
        )
        if not evidence:
            # Backward-compatible fallback.
            evidence = _dedupe_evidence(
                [{"text": value} for value in (
                    list(match.get("left_texts") or [])
                    + list(match.get("right_texts") or [])
                )]
            )
        if not evidence:
            continue

        key = tuple(sorted(
            (_evidence_profile_key(entry), _normalized_text(entry.get("text")))
            for entry in evidence
        ))
        if key in seen:
            continue
        seen.add(key)
        texts = _dedupe_texts([entry["text"] for entry in evidence])
        chunks.append({
            "text": " | ".join(texts),
            "texts": texts,
            "evidence": evidence,
            "source_similarity": round(float(match.get("similarity") or 0.0), 4),
        })
    return chunks

def _item_representative_chunks(
        item: Dict[str, Any],
        limit: int = REPRESENTATIVE_CHUNK_LIMIT,
) -> List[Dict[str, Any]]:
    return [
        chunk
        for chunk in item.get("representative_chunks", [])[:limit]
        if _chunk_text_values(chunk)
    ]


def _original_chunk_registry(links: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    registry: Dict[str, List[Dict[str, Any]]] = {}
    for link in links:
        raw_profiles = link.get("_description_chunks_by_profile") or {}
        if not isinstance(raw_profiles, dict):
            continue
        for fallback_id, raw_profile in raw_profiles.items():
            if not isinstance(raw_profile, dict):
                continue
            profile = {
                "id": str(raw_profile.get("id") or fallback_id or "").strip(),
                "name": str(raw_profile.get("name") or "").strip(),
            }
            profile_key = _profile_key(profile)
            if not profile_key:
                continue
            entries = registry.setdefault(profile_key, [])
            existing = {_normalized_text(entry.get("text")) for entry in entries}
            for value in raw_profile.get("chunks", []) or []:
                text = str(value or "").strip()
                normalized = _normalized_text(text)
                if not normalized or normalized in existing:
                    continue
                existing.add(normalized)
                entries.append({
                    "text": text,
                    "profile_id": profile["id"],
                    "profile_name": profile["name"],
                    "profile_key": profile_key,
                })
    return registry


def _profile_pair_original_chunk_match(
        left_profile: str,
        right_profile: str,
        registry: Dict[str, List[Dict[str, Any]]],
        cache: Dict[tuple, Any],
) -> Optional[Dict[str, Any]]:
    canonical_profiles = tuple(sorted((left_profile, right_profile)))
    if canonical_profiles not in cache:
        first_profile, second_profile = canonical_profiles
        first_entries = registry.get(first_profile, [])
        second_entries = registry.get(second_profile, [])
        if not first_entries or not second_entries:
            cache[canonical_profiles] = None
        else:
            try:
                from sentence_transformers import util
                from dl import similarity

                similarity._ensure_models()
                if similarity._model_long is None:
                    cache[canonical_profiles] = None
                else:
                    first_embedding_key = ("__profile_embeddings__", first_profile)
                    second_embedding_key = ("__profile_embeddings__", second_profile)
                    if first_embedding_key not in cache:
                        cache[first_embedding_key] = similarity._model_long.encode(
                            [entry["text"] for entry in first_entries],
                            convert_to_tensor=True,
                        )
                    if second_embedding_key not in cache:
                        cache[second_embedding_key] = similarity._model_long.encode(
                            [entry["text"] for entry in second_entries],
                            convert_to_tensor=True,
                        )
                    first_embeddings = cache[first_embedding_key]
                    second_embeddings = cache[second_embedding_key]
                    scores = util.cos_sim(first_embeddings, second_embeddings)
                    flat_index = int(scores.argmax().item())
                    first_index, second_index = divmod(flat_index, len(second_entries))
                    score = max(0.0, min(1.0, float(scores[first_index][second_index].item())))
                    cache[canonical_profiles] = {
                        "left_evidence": first_entries[first_index],
                        "right_evidence": second_entries[second_index],
                        "similarity": round(score, 4),
                        "evaluated_chunk_pair_count": len(first_entries) * len(second_entries),
                    }
            except Exception:
                cache[canonical_profiles] = None

    cached = cache.get(canonical_profiles)
    if not cached:
        return None
    if left_profile == canonical_profiles[0]:
        return dict(cached)
    return {
        "left_evidence": cached["right_evidence"],
        "right_evidence": cached["left_evidence"],
        "similarity": cached["similarity"],
        "evaluated_chunk_pair_count": cached["evaluated_chunk_pair_count"],
    }


def _compare_original_profile_chunks(
        left: Dict[str, Any],
        right: Dict[str, Any],
        registry: Dict[str, List[Dict[str, Any]]],
        cache: Dict[tuple, Any],
        limit: int = REPRESENTATIVE_CHUNK_LIMIT,
) -> Dict[str, Any]:
    left_profiles = sorted(_item_profile_keys(left) & set(registry))
    right_profiles = sorted(_item_profile_keys(right) & set(registry))
    matches = []
    seen_profile_pairs = set()

    for left_profile in left_profiles:
        for right_profile in right_profiles:
            if left_profile == right_profile:
                continue
            pair_key = tuple(sorted((left_profile, right_profile)))
            if pair_key in seen_profile_pairs:
                continue
            seen_profile_pairs.add(pair_key)
            match = _profile_pair_original_chunk_match(left_profile, right_profile, registry, cache)
            if not match:
                continue
            left_evidence = dict(match["left_evidence"])
            right_evidence = dict(match["right_evidence"])
            matches.append({
                "chunk_evidence_1": left_evidence["text"],
                "chunk_evidence_2": right_evidence["text"],
                "left_texts": [left_evidence["text"]],
                "right_texts": [right_evidence["text"]],
                "left_evidence": [left_evidence],
                "right_evidence": [right_evidence],
                "atomic_cross_profile_matches": [{
                    "left_profile_id": left_evidence.get("profile_id", ""),
                    "left_profile_name": left_evidence.get("profile_name", ""),
                    "left_profile_key": left_profile,
                    "left_text": left_evidence["text"],
                    "right_profile_id": right_evidence.get("profile_id", ""),
                    "right_profile_name": right_evidence.get("profile_name", ""),
                    "right_profile_key": right_profile,
                    "right_text": right_evidence["text"],
                    "similarity": match["similarity"],
                }],
                "valid_atomic_pair_count": match["evaluated_chunk_pair_count"],
                "similarity": match["similarity"],
                "left_profile_key": left_profile,
                "right_profile_key": right_profile,
            })

    matches.sort(key=lambda item: item["similarity"], reverse=True)
    profile_pair_scores = [
        {
            "left_profile_id": match["left_evidence"][0].get("profile_id", ""),
            "left_profile_name": match["left_evidence"][0].get("profile_name", ""),
            "left_profile_key": match["left_profile_key"],
            "left_text": match["left_evidence"][0]["text"],
            "right_profile_id": match["right_evidence"][0].get("profile_id", ""),
            "right_profile_name": match["right_evidence"][0].get("profile_name", ""),
            "right_profile_key": match["right_profile_key"],
            "right_text": match["right_evidence"][0]["text"],
            "similarity": match["similarity"],
        }
        for match in matches
    ]
    top_matches = matches[:limit]
    for rank, match in enumerate(top_matches, 1):
        match["rank"] = rank

    return {
        "similarity": _average([match["similarity"] for match in top_matches]),
        "matches": top_matches,
        "excluded_self_pairs": [],
        "profile_pair_scores": profile_pair_scores,
        "represented_left_profiles": left_profiles,
        "represented_right_profiles": right_profiles,
        "evidence_strategy": "all_original_profile_chunks_cached",
        "evaluated_profile_pair_count": len(profile_pair_scores),
    }


def _item_representative_keywords(item: Dict[str, Any]) -> Set[str]:
    keyword_evidence = item.get("keyword_evidence")
    if isinstance(keyword_evidence, dict) and keyword_evidence:
        return set(keyword_evidence.keys())
    values = item.get("representative_keywords")
    if values is None:
        values = (item.get("evidence") or {}).get("shared_keywords", [])
    return _as_set(values)

def _representative_chunk_embeddings(model: Any, texts: List[str]):
    # Kept as a small compatibility helper. Recursive comparison now embeds
    # atomic provenance-aware evidence texts rather than concatenated chunks.
    return model.encode(texts, convert_to_tensor=True)

def _compare_representative_chunks(
        left: Dict[str, Any],
        right: Dict[str, Any],
        limit: int = REPRESENTATIVE_CHUNK_LIMIT,
) -> Dict[str, Any]:
    left_chunks = _item_representative_chunks(left, limit)
    right_chunks = _item_representative_chunks(right, limit)
    if not left_chunks or not right_chunks:
        return {
            "similarity": 0.0,
            "matches": [],
            "excluded_self_pairs": [],
            "profile_pair_scores": [],
            "represented_left_profiles": [],
            "represented_right_profiles": [],
        }

    left_evidence = [_chunk_evidence_entries(chunk) for chunk in left_chunks]
    right_evidence = [_chunk_evidence_entries(chunk) for chunk in right_chunks]
    all_entries = [entry for group in left_evidence + right_evidence for entry in group]
    if not all_entries:
        return {
            "similarity": 0.0,
            "matches": [],
            "excluded_self_pairs": [],
            "profile_pair_scores": [],
            "represented_left_profiles": [],
            "represented_right_profiles": [],
        }

    # Encode every unique atomic text only once.
    text_map = {}
    for entry in all_entries:
        norm = _normalized_text(entry.get("text"))
        if norm and norm not in text_map:
            text_map[norm] = entry.get("text")

    try:
        from sentence_transformers import util
        from dl import similarity

        similarity._ensure_models()
        if similarity._model_long is None:
            return {
                "similarity": 0.0,
                "matches": [],
                "excluded_self_pairs": [],
                "profile_pair_scores": [],
                "represented_left_profiles": [],
                "represented_right_profiles": [],
            }

        norms = list(text_map)
        embeddings = _representative_chunk_embeddings(
            similarity._model_long,
            [text_map[norm] for norm in norms],
        )
        norm_to_idx = {norm: idx for idx, norm in enumerate(norms)}
        score_matrix = util.cos_sim(embeddings, embeddings)
    except Exception:
        return {
            "similarity": 0.0,
            "matches": [],
            "excluded_self_pairs": [],
            "profile_pair_scores": [],
            "represented_left_profiles": [],
            "represented_right_profiles": [],
        }

    excluded_self_pairs = []
    matches = []
    profile_pair_best: Dict[tuple, Dict[str, Any]] = {}
    represented_left_profiles = {
        _evidence_profile_key(entry)
        for group in left_evidence
        for entry in group
        if _evidence_profile_key(entry)
    }
    represented_right_profiles = {
        _evidence_profile_key(entry)
        for group in right_evidence
        for entry in group
        if _evidence_profile_key(entry)
    }

    for left_idx, left_chunk in enumerate(left_chunks):
        for right_idx, right_chunk in enumerate(right_chunks):
            allowed_scores = []
            atomic_matches = []

            for left_entry in left_evidence[left_idx]:
                for right_entry in right_evidence[right_idx]:
                    left_profile = _evidence_profile_key(left_entry)
                    right_profile = _evidence_profile_key(right_entry)
                    left_norm = _normalized_text(left_entry.get("text"))
                    right_norm = _normalized_text(right_entry.get("text"))

                    # Provenance-aware self-match filter: AB vs BC may use
                    # A↔B, A↔C and B↔C evidence, but never B↔B. If provenance
                    # is unavailable, suppress only exact-text self matches.
                    same_profile = bool(left_profile and right_profile and left_profile == right_profile)
                    unknown_exact_self = bool(
                        not left_profile and not right_profile and left_norm and left_norm == right_norm
                    )
                    if same_profile or unknown_exact_self:
                        excluded_self_pairs.append({
                            "left_profile": left_profile,
                            "right_profile": right_profile,
                            "left_text": left_entry.get("text", ""),
                            "right_text": right_entry.get("text", ""),
                        })
                        continue
                    if left_norm not in norm_to_idx or right_norm not in norm_to_idx:
                        continue

                    score = max(0.0, min(1.0, float(
                        score_matrix[norm_to_idx[left_norm]][norm_to_idx[right_norm]].item()
                    )))
                    allowed_scores.append(score)
                    atomic_match = {
                        "left_profile_id": left_entry.get("profile_id", ""),
                        "left_profile_name": left_entry.get("profile_name", ""),
                        "left_profile_key": left_profile,
                        "left_text": left_entry.get("text", ""),
                        "right_profile_id": right_entry.get("profile_id", ""),
                        "right_profile_name": right_entry.get("profile_name", ""),
                        "right_profile_key": right_profile,
                        "right_text": right_entry.get("text", ""),
                        "similarity": round(score, 4),
                    }
                    atomic_matches.append(atomic_match)

                    # Keep the strongest observed atomic semantic relation for
                    # every unique cross-group profile pair. This summary is
                    # later used to validate latent bridges such as AB + CD.
                    if left_profile and right_profile:
                        profile_pair_key = (left_profile, right_profile)
                        previous = profile_pair_best.get(profile_pair_key)
                        if previous is None or score > float(previous.get("similarity") or 0.0):
                            profile_pair_best[profile_pair_key] = dict(atomic_match)

            if not allowed_scores:
                continue

            # One representative-chunk pair gets one conservative score: the
            # mean of all valid cross-profile evidence pairs. Only after that
            # do we apply the user's 3x3 -> top-3 -> mean rule.
            pair_similarity = round(sum(allowed_scores) / len(allowed_scores), 4)
            atomic_matches.sort(key=lambda item: item["similarity"], reverse=True)
            matches.append({
                "chunk_evidence_1": left_chunk.get("text", ""),
                "chunk_evidence_2": right_chunk.get("text", ""),
                "left_texts": _chunk_text_values(left_chunk),
                "right_texts": _chunk_text_values(right_chunk),
                "left_evidence": left_evidence[left_idx],
                "right_evidence": right_evidence[right_idx],
                "atomic_cross_profile_matches": atomic_matches[:3],
                "valid_atomic_pair_count": len(allowed_scores),
                "similarity": pair_similarity,
            })

    matches = sorted(matches, key=lambda item: item["similarity"], reverse=True)[:limit]
    for rank, match in enumerate(matches, 1):
        match["rank"] = rank

    profile_pair_scores = sorted(
        profile_pair_best.values(),
        key=lambda item: item.get("similarity", 0.0),
        reverse=True,
    )

    return {
        "similarity": _average([match["similarity"] for match in matches]),
        "matches": matches,
        "excluded_self_pairs": excluded_self_pairs,
        "profile_pair_scores": profile_pair_scores,
        "represented_left_profiles": sorted(represented_left_profiles),
        "represented_right_profiles": sorted(represented_right_profiles),
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


def _latent_semantic_bridge_details(
        chunk_comparison: Dict[str, Any],
        keyword_similarity: float,
        relation_threshold: float,
        latent_relation_threshold: float = LATENT_RELATION_THRESHOLD,
        latent_profile_pair_threshold: float = LATENT_PROFILE_PAIR_THRESHOLD,
        latent_min_strong_profile_pairs: int = LATENT_MIN_STRONG_PROFILE_PAIRS,
        latent_min_profile_coverage: float = LATENT_MIN_PROFILE_COVERAGE,
) -> Dict[str, Any]:
    """Validate a hidden semantic bridge between disjoint parent groups.

    We score unique profile-to-profile relations rather than allowing repeated
    chunk matches from one lucky pair to dominate. Coverage is measured over
    the profiles represented by the chunk evidence available to the merge.
    """
    pair_scores = list(chunk_comparison.get("profile_pair_scores") or [])
    left_profiles = set(chunk_comparison.get("represented_left_profiles") or [])
    right_profiles = set(chunk_comparison.get("represented_right_profiles") or [])
    possible_pair_count = len(left_profiles) * len(right_profiles)

    strong_pairs = [
        item for item in pair_scores
        if float(item.get("similarity") or 0.0) >= latent_profile_pair_threshold
    ]
    strong_pair_count = len(strong_pairs)
    coverage = (strong_pair_count / possible_pair_count) if possible_pair_count else 0.0
    strong_pair_avg = _average([float(item.get("similarity") or 0.0) for item in strong_pairs])

    # Use the strongest *unique profile-pair* relations as the latent chunk
    # signal. This is more robust than reusing three chunk matches that may all
    # be driven by the same profile pair.
    latent_chunk_pairs = pair_scores[:REPRESENTATIVE_CHUNK_LIMIT]
    latent_chunk_similarity = _average([
        float(item.get("similarity") or 0.0)
        for item in latent_chunk_pairs
    ])
    latent_merge_similarity = round(
        RECURSIVE_CHUNK_WEIGHT * latent_chunk_similarity
        + RECURSIVE_KEYWORD_WEIGHT * keyword_similarity,
        4,
    )
    applied_threshold = max(float(relation_threshold), float(latent_relation_threshold))

    allowed = bool(
        possible_pair_count
        and strong_pair_count >= max(1, int(latent_min_strong_profile_pairs))
        and coverage >= float(latent_min_profile_coverage)
        and strong_pair_avg >= float(latent_profile_pair_threshold)
        and latent_merge_similarity >= applied_threshold
    )

    return {
        "allowed": allowed,
        "merge_similarity": latent_merge_similarity,
        "chunk_similarity": round(latent_chunk_similarity, 4),
        "keyword_similarity": round(keyword_similarity, 4),
        "applied_threshold": round(applied_threshold, 4),
        "profile_pair_threshold": round(float(latent_profile_pair_threshold), 4),
        "minimum_strong_profile_pairs": max(1, int(latent_min_strong_profile_pairs)),
        "minimum_profile_coverage": round(float(latent_min_profile_coverage), 4),
        "possible_profile_pair_count": possible_pair_count,
        "strong_profile_pair_count": strong_pair_count,
        "profile_pair_coverage": round(coverage, 4),
        "strong_profile_pair_similarity_avg": round(strong_pair_avg, 4),
        "top_profile_pair_matches": latent_chunk_pairs,
        "strong_profile_pair_matches": strong_pairs,
    }


def _compare_level_items(
        left: Dict[str, Any],
        right: Dict[str, Any],
        comparisons: List[Dict[str, Any]],
        level_number: int,
        relation_threshold: float,
        latent_relation_threshold: float = LATENT_RELATION_THRESHOLD,
        latent_profile_pair_threshold: float = LATENT_PROFILE_PAIR_THRESHOLD,
        latent_min_strong_profile_pairs: int = LATENT_MIN_STRONG_PROFILE_PAIRS,
        latent_min_profile_coverage: float = LATENT_MIN_PROFILE_COVERAGE,
        original_chunk_registry: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        original_chunk_cache: Optional[Dict[tuple, Any]] = None,
        pair_lookup: Optional[Dict[tuple, Dict[str, Any]]] = None,
        profile_chunks: Optional[Dict[str, List[str]]] = None,
        profile_labels: Optional[Dict[str, str]] = None,
        profile_chunk_cache: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    left_refs = _item_link_refs(left)
    right_refs = _item_link_refs(right)
    merged_refs = left_refs | right_refs
    if not left_refs or not right_refs or merged_refs == left_refs or merged_refs == right_refs:
        return None

    left_profiles = _item_profile_keys(left)
    right_profiles = _item_profile_keys(right)
    merged_profiles = left_profiles | right_profiles
    shared_parent_profiles = left_profiles & right_profiles
    merge_type = "shared_profile_merge"

    # A higher level may only merge overlapping parent groups. Disjoint
    # bridges would create a new comparison instead of reusing prior evidence.
    if not shared_parent_profiles:
        return None

    if pair_lookup is not None:
        internal_links = []
        missing_pairs = []
        cached_pair_metrics = {}
        reused_pair_count = 0
        new_links = []
        for parent in (left, right):
            for metric in (parent.get("mergeEvidence") or {}).get("internal_pair_metrics", []) or []:
                pair = metric.get("pair") or []
                if len(pair) == 2:
                    cached_pair_metrics[_profile_pair_key(pair[0], pair[1])] = metric

        pair_metrics = []
        for profile_left, profile_right in combinations(sorted(merged_profiles), 2):
            pair_key = _profile_pair_key(profile_left, profile_right)
            cached_metric = cached_pair_metrics.get(pair_key)
            pair = pair_lookup.get(pair_key)
            if cached_metric is not None:
                pair_metrics.append(dict(cached_metric))
                reused_pair_count += 1
                if pair is not None:
                    internal_links.append(pair)
                continue
            if pair is None:
                missing_pairs.append([profile_left, profile_right])
            else:
                internal_links.append(pair)
                new_links.append(pair)
                metrics = pair.get("metrics") or {}
                try:
                    combined_similarity = float(metrics.get("combined_similarity", 0)) / 100.0
                    keyword_similarity = float(metrics.get("keywords_similarity", 0)) / 100.0
                    chunk_similarity = float(metrics.get("description_similarity", 0)) / 100.0
                except (TypeError, ValueError):
                    continue
                pair_metrics.append({
                    "pair": [profile_left, profile_right],
                    "source_link": pair.get("@id"),
                    "combined_similarity": round(combined_similarity, 4),
                    "keyword_similarity": round(keyword_similarity, 4),
                    "chunk_similarity": round(chunk_similarity, 4),
                    "shared_keywords": sorted(_keyword_evidence_from_links([pair])),
                })
        if missing_pairs:
            return None

        chunk_scores = []
        keyword_scores = []
        combined_scores = []
        chunk_matches = []
        pair_statuses = []
        inherited_chunk_matches = []
        inherited_keywords = set()
        for parent in (left, right):
            parent_evidence = parent.get("mergeEvidence") or {}
            inherited_chunk_matches.extend(parent_evidence.get("description_chunk_matches", []) or [])
            inherited_keywords.update(_as_set(parent_evidence.get("shared_keywords", [])))
        for metric in pair_metrics:
            combined_scores.append(float(metric.get("combined_similarity") or 0.0))
            keyword_scores.append(float(metric.get("keyword_similarity") or 0.0))
            chunk_scores.append(float(metric.get("chunk_similarity") or 0.0))
            pair_statuses.append({
                "pair": metric.get("pair", []),
                "status": "green" if combined_scores[-1] >= relation_threshold else "red",
                "similarity": round(combined_scores[-1], 4),
            })
            inherited_keywords.update(_as_set(metric.get("shared_keywords", [])))

        for link in new_links:
            chunk_matches.extend((link.get("description_top_chunks") or [])[:3])

        if not combined_scores:
            return None
        chunk_similarity = _average(chunk_scores)
        keyword_similarity = _average(keyword_scores)
        relation_similarity = round(
            RECURSIVE_CHUNK_WEIGHT * chunk_similarity
            + RECURSIVE_KEYWORD_WEIGHT * keyword_similarity,
            4,
        )
        # Fill missing profile chunks from the complete original registry so
        # large composites can show one shared chunk group across all members.
        evidence_chunks = dict(profile_chunks or {}) if profile_chunks is not None else {}
        evidence_labels = dict(profile_labels or {})
        for profile_key in merged_profiles if profile_chunks is not None else []:
            entries = original_chunk_registry.get(profile_key, [])
            if not evidence_chunks.get(profile_key):
                evidence_chunks[profile_key] = [entry.get("text", "") for entry in entries]
            if not evidence_labels.get(profile_key) and entries:
                evidence_labels[profile_key] = entries[0].get("profile_name") or profile_key
        # Multi-profile groups are attached once to terminal composites below.
        # Computing them for every intermediate merge is quadratic in the number
        # of candidate links and dominates large (100+ profile) builds.
        multi_profile_matches = []
        return {
            "@type": "RecursiveCompositeComparison",
            "@id": f"recursive-comparison:{uuid.uuid4()}",
            "recursive_level": level_number,
            "leftRef": left.get("id"),
            "rightRef": right.get("id"),
            "leftOriginRefs": _item_origin_refs(left),
            "rightOriginRefs": _item_origin_refs(right),
            "originLinkRefs": sorted(merged_refs),
            "merge_type": merge_type,
            "sharedParentProfiles": sorted(shared_parent_profiles),
            "bridgeComparisonRefs": [],
            "relation_similarity": relation_similarity,
            "merge_similarity": relation_similarity,
            "status": "green" if relation_similarity >= relation_threshold else "red",
            "chunk_similarity": chunk_similarity,
            "keyword_similarity": keyword_similarity,
            "applied_threshold": round(float(relation_threshold), 4),
            "scoring_weights": {
                "chunks": RECURSIVE_CHUNK_WEIGHT,
                "keywords": RECURSIVE_KEYWORD_WEIGHT,
            },
            "evidence": {
                "shared_profiles": [],
                "shared_parent_profiles": sorted(shared_parent_profiles),
                 "shared_keywords": sorted(inherited_keywords),
                 "description_chunk_matches": (inherited_chunk_matches + chunk_matches)[:3],
                "multi_profile_chunk_matches": multi_profile_matches,
                "chunk_evidence_strategy": (
                    "multi_profile_anchor_chunks" if multi_profile_matches
                    else "cached_base_pair_metrics"
                ),
                 "evaluated_profile_pair_count": len(pair_metrics),
                 "reused_profile_pair_count": reused_pair_count,
                 "new_profile_pair_count": len(pair_metrics) - reused_pair_count,
                 "internal_pair_metrics": pair_metrics,
                 "profile_pair_chunk_matches": pair_metrics,
                "internal_pair_statuses": pair_statuses,
                "internal_pair_scores": [round(score, 4) for score in combined_scores],
            },
        }

    bridges = _bridge_comparisons(left_refs, right_refs, comparisons)
    if original_chunk_registry:
        chunk_comparison = _compare_original_profile_chunks(
            left,
            right,
            original_chunk_registry,
            original_chunk_cache if original_chunk_cache is not None else {},
        )
    else:
        chunk_comparison = _compare_representative_chunks(left, right)
    structural_chunk_similarity = chunk_comparison["similarity"]

    left_keyword_evidence = _item_keyword_evidence(left)
    right_keyword_evidence = _item_keyword_evidence(right)
    left_keywords = set(left_keyword_evidence)
    right_keywords = set(right_keyword_evidence)
    direct_shared_keywords = left_keywords & right_keywords
    keyword_similarity = _jaccard(left_keywords, right_keywords)

    bridge_keywords = set()
    shared_profiles = set()
    bridge_similarities = []
    for comparison in bridges:
        evidence = comparison.get("evidence") or {}
        bridge_keywords.update(_as_set(evidence.get("shared_keywords", [])))
        shared_profiles.update(evidence.get("shared_profiles", []) or [])
        try:
            bridge_similarities.append(float(comparison.get("relation_similarity") or 0.0))
        except (TypeError, ValueError):
            pass

    shared_keyword_support = []
    for key in sorted(direct_shared_keywords):
        left_support = _keyword_support_keys(left_keyword_evidence.get(key, {}))
        right_support = _keyword_support_keys(right_keyword_evidence.get(key, {}))
        shared_keyword_support.append({
            "keyword": (
                left_keyword_evidence.get(key, {}).get("keyword")
                or right_keyword_evidence.get(key, {}).get("keyword")
                or key
            ),
            "left_profile_support": sorted(left_support),
            "right_profile_support": sorted(right_support),
            "cross_profile_support": bool(
                any(lp != rp for lp in left_support for rp in right_support)
            ),
        })

    latent_bridge = None
    chunk_similarity = structural_chunk_similarity
    relation_similarity = round(
        RECURSIVE_CHUNK_WEIGHT * chunk_similarity
        + RECURSIVE_KEYWORD_WEIGHT * keyword_similarity,
        4,
    )
    applied_threshold = float(relation_threshold)

    display_shared_keywords = sorted({
        left_keyword_evidence.get(key, {}).get("keyword")
        or right_keyword_evidence.get(key, {}).get("keyword")
        or key
        for key in direct_shared_keywords
    })

    evidence = {
        "merge_type": merge_type,
        "shared_profiles": sorted(shared_profiles),
        "shared_parent_profiles": sorted(shared_parent_profiles),
        "shared_keywords": display_shared_keywords,
        "direct_shared_keywords": display_shared_keywords,
        "shared_keyword_support": shared_keyword_support,
        "bridge_shared_keywords": sorted(bridge_keywords),
        "bridge_relation_similarity_avg": _average(bridge_similarities),
        "excluded_self_chunk_pairs": chunk_comparison.get("excluded_self_pairs", []),
        "description_chunk_matches": chunk_comparison["matches"],
        "cross_profile_pair_scores": chunk_comparison.get("profile_pair_scores", []),
        "profile_pair_chunk_matches": chunk_comparison.get("profile_pair_scores", []),
        "chunk_evidence_strategy": chunk_comparison.get(
            "evidence_strategy",
            "recursive_representative_chunks",
        ),
        "evaluated_profile_pair_count": chunk_comparison.get("evaluated_profile_pair_count", 0),
    }
    if latent_bridge is not None:
        evidence["latent_semantic_bridge"] = latent_bridge

    return {
        "@type": "RecursiveCompositeComparison",
        "@id": f"recursive-comparison:{uuid.uuid4()}",
        "recursive_level": level_number,
        "leftRef": left.get("id"),
        "rightRef": right.get("id"),
        "leftOriginRefs": _item_origin_refs(left),
        "rightOriginRefs": _item_origin_refs(right),
        "originLinkRefs": sorted(merged_refs),
        "merge_type": merge_type,
        "sharedParentProfiles": sorted(shared_parent_profiles),
        "bridgeComparisonRefs": [comparison.get("@id") for comparison in bridges if comparison.get("@id")],
        "relation_similarity": round(relation_similarity, 4),
        "merge_similarity": round(relation_similarity, 4),
        "status": "green" if relation_similarity >= applied_threshold else "red",
        "chunk_similarity": round(chunk_similarity, 4),
        "keyword_similarity": round(keyword_similarity, 4),
        "applied_threshold": round(applied_threshold, 4),
        "scoring_weights": {
            "chunks": RECURSIVE_CHUNK_WEIGHT,
            "keywords": RECURSIVE_KEYWORD_WEIGHT,
        },
        "evidence": evidence,
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
    merge_similarity = float(recursive_comparison.get("relation_similarity") or 0.0)
    parent_recursive_coherences = _recursive_parent_coherences(parent_items)

    # Start recursive coherence at the first recursive merge. From level 3 on,
    # enforce monotonicity against parent recursive coherence. Do not mix the
    # legacy base-link combined score with this new recursive metric.
    if parent_recursive_coherences:
        recursive_coherence = round(min(parent_recursive_coherences + [merge_similarity]), 4)
    else:
        recursive_coherence = round(merge_similarity, 4)

    keyword_evidence = _keyword_evidence_from_links(links)
    representative_keywords = sorted(
        item.get("keyword") or key
        for key, item in keyword_evidence.items()
    )

    evidence["recursive_chunk_similarity"] = recursive_comparison.get("chunk_similarity", 0.0)
    evidence["recursive_keyword_similarity"] = recursive_comparison.get("keyword_similarity", 0.0)
    evidence["merge_similarity"] = merge_similarity
    evidence["parent_recursive_coherences"] = parent_recursive_coherences
    evidence["recursive_coherence"] = recursive_coherence
    evidence["composite_coherence"] = recursive_coherence
    evidence["chunk_similarity_avg"] = recursive_comparison.get("chunk_similarity", 0.0)

    profiles = [profile_map[key] for key in sorted(profile_keys) if key in profile_map]
    return {
        "@type": "HierarchyLevelItem",
        "item_type": "CompositeDataLinkingElement",
        "id": f"hierarchy:level:{level_number}:{uuid.uuid4()}",
         "label": f"Composite of {len(profiles)} profile(s)",
        "similarity": recursive_coherence,
        "status": recursive_comparison.get("status", "green"),
        "recursive_coherence": recursive_coherence,
        "merge_similarity": merge_similarity,
        "linkRefs": sorted(link_ids),
        "internalLinkRefs": sorted(link_ids),
        "originRefs": sorted(link_ids),
        "comparisonRefs": sorted(item for item in comparison_ids if item),
        "parentRefs": [item.get("id") for item in parent_items if item.get("id")],
        "recursiveComparisonRef": recursive_comparison.get("@id"),
        "supportingComparisons": [recursive_comparison],
        "recursive_level": level_number,
        "representative_chunks": _representative_chunks_from_matches(
            recursive_evidence.get("description_chunk_matches", [])
        ),
        "representative_keywords": representative_keywords,
        "keyword_evidence": keyword_evidence,
        "profiles": profiles,
        "mergeEvidence": {
            "recursiveComparisonRef": recursive_comparison.get("@id"),
            "bridgeComparisonRefs": recursive_comparison.get("bridgeComparisonRefs", []),
            "merge_type": recursive_comparison.get("merge_type", "shared_profile_bridge"),
            "applied_threshold": recursive_comparison.get("applied_threshold"),
            "latent_semantic_bridge": recursive_evidence.get("latent_semantic_bridge"),
            "merge_similarity": merge_similarity,
            "recursive_coherence": recursive_coherence,
            "composite_coherence": recursive_coherence,
            "chunk_similarity": recursive_comparison.get("chunk_similarity", 0.0),
            "keyword_similarity": recursive_comparison.get("keyword_similarity", 0.0),
            "scoring_weights": recursive_comparison.get("scoring_weights", {}),
            "description_chunk_matches": recursive_evidence.get("description_chunk_matches", []),
            "multi_profile_chunk_matches": recursive_evidence.get("multi_profile_chunk_matches", []),
            "chunk_evidence_strategy": recursive_evidence.get("chunk_evidence_strategy"),
             "evaluated_profile_pair_count": recursive_evidence.get("evaluated_profile_pair_count", 0),
             "reused_profile_pair_count": recursive_evidence.get("reused_profile_pair_count", 0),
             "new_profile_pair_count": recursive_evidence.get("new_profile_pair_count", 0),
            "internal_pair_metrics": recursive_evidence.get("internal_pair_metrics", []),
            "profile_pair_chunk_matches": recursive_evidence.get("profile_pair_chunk_matches", []),
            "shared_keywords": recursive_evidence.get("shared_keywords", []),
        },
        "evidence": evidence,
    }

def _merge_duplicate_composite_item(
        item: Dict[str, Any],
        recursive_comparison: Dict[str, Any],
        parent_items: List[Dict[str, Any]],
        links_by_id: Dict[str, Dict[str, Any]],
        comparisons: List[Dict[str, Any]],
) -> None:
    supporting = item.setdefault("supportingComparisons", [])
    supporting.append(recursive_comparison)

    merge_similarity = float(recursive_comparison.get("relation_similarity") or 0.0)
    parent_recursive_coherences = _recursive_parent_coherences(parent_items)
    if parent_recursive_coherences:
        candidate_coherence = round(min(parent_recursive_coherences + [merge_similarity]), 4)
    else:
        candidate_coherence = round(merge_similarity, 4)

    item["parentRefs"] = sorted({
        *(item.get("parentRefs") or []),
        *(parent.get("id") for parent in parent_items if parent.get("id")),
    })

    # Several parent-pair routes can create the same profile group. Keep every
    # route as support but select the route with the strongest recursive
    # coherence as the primary representative evidence.
    if candidate_coherence > float(item.get("recursive_coherence") or item.get("similarity") or 0.0):
        recursive_evidence = recursive_comparison.get("evidence") or {}
        item["similarity"] = candidate_coherence
        item["status"] = recursive_comparison.get("status", "green")
        item["recursive_coherence"] = candidate_coherence
        item["merge_similarity"] = merge_similarity
        item["recursiveComparisonRef"] = recursive_comparison.get("@id")
        item["representative_chunks"] = _representative_chunks_from_matches(
            recursive_evidence.get("description_chunk_matches", [])
        )
        item["mergeEvidence"] = {
            "recursiveComparisonRef": recursive_comparison.get("@id"),
            "bridgeComparisonRefs": recursive_comparison.get("bridgeComparisonRefs", []),
            "merge_type": recursive_comparison.get("merge_type", "shared_profile_bridge"),
            "applied_threshold": recursive_comparison.get("applied_threshold"),
            "latent_semantic_bridge": recursive_evidence.get("latent_semantic_bridge"),
            "merge_similarity": merge_similarity,
            "recursive_coherence": candidate_coherence,
            "composite_coherence": candidate_coherence,
            "chunk_similarity": recursive_comparison.get("chunk_similarity", 0.0),
            "keyword_similarity": recursive_comparison.get("keyword_similarity", 0.0),
            "scoring_weights": recursive_comparison.get("scoring_weights", {}),
            "description_chunk_matches": recursive_evidence.get("description_chunk_matches", []),
            "multi_profile_chunk_matches": recursive_evidence.get("multi_profile_chunk_matches", []),
            "chunk_evidence_strategy": recursive_evidence.get("chunk_evidence_strategy"),
             "evaluated_profile_pair_count": recursive_evidence.get("evaluated_profile_pair_count", 0),
             "reused_profile_pair_count": recursive_evidence.get("reused_profile_pair_count", 0),
             "new_profile_pair_count": recursive_evidence.get("new_profile_pair_count", 0),
             "internal_pair_metrics": recursive_evidence.get("internal_pair_metrics", []),
             "profile_pair_chunk_matches": recursive_evidence.get("profile_pair_chunk_matches", []),
             "shared_keywords": recursive_evidence.get("shared_keywords", []),
        }

    link_refs = set(item.get("linkRefs", [])) | set(recursive_comparison.get("originLinkRefs", []))
    item["linkRefs"] = sorted(link_refs)
    item["internalLinkRefs"] = sorted(link_refs)
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
    item["label"] = f"Composite of {len(item['profiles'])} profile(s)"

    keyword_evidence = _keyword_evidence_from_links(valid_links)
    item["keyword_evidence"] = keyword_evidence
    item["representative_keywords"] = sorted(
        value.get("keyword") or key
        for key, value in keyword_evidence.items()
    )

    evidence = _composite_evidence(valid_links, component_comparisons)
    evidence["recursive_chunk_similarity"] = item.get("mergeEvidence", {}).get("chunk_similarity", 0.0)
    evidence["recursive_keyword_similarity"] = item.get("mergeEvidence", {}).get("keyword_similarity", 0.0)
    evidence["merge_similarity"] = item.get("merge_similarity", 0.0)
    evidence["recursive_coherence"] = item.get("recursive_coherence", item.get("similarity", 0.0))
    evidence["composite_coherence"] = evidence["recursive_coherence"]
    evidence["supporting_recursive_comparison_count"] = len(supporting)
    evidence["chunk_similarity_avg"] = evidence["recursive_chunk_similarity"]
    item["evidence"] = evidence

def _build_hierarchy_levels(
        links: List[Dict[str, Any]],
        comparisons: List[Dict[str, Any]],
        relation_threshold: float,
        base_threshold: Optional[float] = None,
        profile_chunks: Optional[Dict[str, List[str]]] = None,
        profile_labels: Optional[Dict[str, str]] = None,
        profile_chunk_cache: Optional[Dict[str, Any]] = None,
        latent_relation_threshold: float = LATENT_RELATION_THRESHOLD,
        latent_profile_pair_threshold: float = LATENT_PROFILE_PAIR_THRESHOLD,
        latent_min_strong_profile_pairs: int = LATENT_MIN_STRONG_PROFILE_PAIRS,
        latent_min_profile_coverage: float = LATENT_MIN_PROFILE_COVERAGE,
        original_chunk_cache: Optional[Dict[tuple, Any]] = None,
) -> tuple:
    links_by_id = {link.get("@id"): link for link in links if link.get("@id")}
    pair_lookup = {}
    for link in links:
        profiles = _profile_entries(link)
        if len(profiles) == 2:
            pair_lookup[_profile_pair_key(_profile_key(profiles[0]), _profile_key(profiles[1]))] = link
    base_items = [_link_level_item(link, base_threshold if base_threshold is not None else relation_threshold) for link in links]
    original_chunk_registry = _original_chunk_registry(links)
    original_chunk_cache = original_chunk_cache if original_chunk_cache is not None else {}
    candidate_items = [item for item in base_items if item.get("status") == "green"]
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

        # Important: compare only items from the immediately previous level.
        # This prevents old base items from being repeatedly reintroduced at
        # every recursive depth and keeps the hierarchy truly level-by-level.
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
            # Pair neighbouring groups only when they share the overlap needed
            # to add one new profile (AB+BC -> ABC, ABC+ABD -> ABCD).
            if len(left_profiles) != len(right_profiles):
                continue
            if len(left_profiles & right_profiles) != len(left_profiles) - 1:
                continue
            if group_key in seen_groups:
                continue

            recursive_comparison = _compare_level_items(
                left,
                right,
                comparisons,
                current_level,
                relation_threshold,
                latent_relation_threshold,
                latent_profile_pair_threshold,
                latent_min_strong_profile_pairs,
                latent_min_profile_coverage,
                original_chunk_registry,
                original_chunk_cache,
                pair_lookup,
                profile_chunks,
                profile_labels,
                profile_chunk_cache,
            )
            if not recursive_comparison:
                continue

            if group_key in next_groups:
                _merge_duplicate_composite_item(
                    next_groups[group_key],
                    recursive_comparison,
                    [left, right],
                    links_by_id,
                    comparisons,
                )
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
            key=lambda item: (len(item.get("profiles", [])), item.get("similarity", 0.0)),
            reverse=True,
        )
        levels.append({
            "level": current_level,
            "name": f"Composite merge level {current_level - 1}",
            "description": (
                "CompositeDataLinkingElement items produced only from the previous level, "
                "using cached original profile-chunk similarity plus persistent keyword overlap."
            ),
            "items": next_items,
        })
        seen_groups.update(next_groups)
        candidate_items = [item for item in next_items if item.get("status") == "green"]
        current_level += 1

    return levels, recursive_comparisons


def _attach_terminal_chunk_evidence(
        items: List[Dict[str, Any]],
        profile_chunks: Optional[Dict[str, List[str]]],
        profile_labels: Optional[Dict[str, str]],
        profile_chunk_cache: Optional[Dict[str, Any]],
) -> None:
    if profile_chunks is None:
        return
    registry = profile_chunks
    labels = profile_labels or {}
    for item in items:
        profiles = item.get("profiles", []) or []
        profile_keys = sorted(
            key for key in (_profile_key(profile) for profile in profiles) if key
        )
        if not profile_keys:
            continue
        chunks = {key: list(registry.get(key, []) or []) for key in profile_keys}
        matches = _multi_profile_chunk_evidence(
            profile_keys,
            chunks,
            {key: labels.get(key, key) for key in profile_keys},
            cache=profile_chunk_cache,
        )
        merge_evidence = item.setdefault("mergeEvidence", {})
        merge_evidence["multi_profile_chunk_matches"] = matches
        if matches:
            merge_evidence["chunk_evidence_strategy"] = "multi_profile_anchor_chunks"

def _composite_from_level_item(item: Dict[str, Any]) -> Dict[str, Any]:
    recursive_coherence = item.get("recursive_coherence", item.get("similarity", 0.0))
    profiles = item.get("profiles", []) or []
    profile_count = len(profiles)
    merge_evidence = item.get("mergeEvidence", {}) or {}
    internal_pair_count = merge_evidence.get(
        "evaluated_profile_pair_count",
        profile_count * (profile_count - 1) // 2,
    )
    return {
        "@type": "CompositeDataLinkingElement",
        "@id": item.get("id"),
        "is_terminal": True,
        "profiles": profiles,
        "linkRefs": item.get("linkRefs", []),
        "internalLinkRefs": item.get("internalLinkRefs", item.get("linkRefs", [])),
        "comparisonRefs": item.get("comparisonRefs", []),
        "parentRefs": item.get("parentRefs", []),
        "link_count": len(item.get("linkRefs", [])),
        "source_link_count": len(item.get("linkRefs", [])),
        "internal_pair_count": internal_pair_count,
        "profile_count": profile_count,
        "comparison_count": len(item.get("comparisonRefs", [])),
        "composite_coherence": recursive_coherence,
        "recursive_coherence": recursive_coherence,
        "recursive_level": item.get("recursive_level"),
        "recursiveComparisonRef": item.get("recursiveComparisonRef"),
        "supportingComparisons": item.get("supportingComparisons", []),
        "representative_chunks": item.get("representative_chunks", []),
        "representative_keywords": item.get("representative_keywords", []),
        "keyword_evidence": item.get("keyword_evidence", {}),
        "merge_similarity": item.get("merge_similarity", 0.0),
        "evidence": item.get("evidence", {}),
        "mergeEvidence": item.get("mergeEvidence", {}),
    }

def _final_composite_items(levels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return green groups from the last level that produced green groups."""
    green_levels = [
        level
        for level in levels[1:]
        if any(
            item.get("item_type") == "CompositeDataLinkingElement"
            and item.get("status") == "green"
            for item in level.get("items", [])
        )
    ]
    if not green_levels:
        return []

    last_green_level = green_levels[-1]
    return [
        item
        for item in last_green_level.get("items", [])
        if item.get("item_type") == "CompositeDataLinkingElement"
        and item.get("status") == "green"
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
    profile_count = len(profile_keys)
    return {
        "@type": "CompositeDataLinkingElement",
        "@id": f"composite:{uuid.uuid4()}",
        "profiles": [profile_map[key] for key in sorted(profile_keys) if key in profile_map],
        "linkRefs": sorted(component_link_ids),
        "comparisonRefs": sorted(comparison.get("@id") for comparison in component_comparisons if comparison.get("@id")),
        "link_count": len(component_links),
        "source_link_count": len(component_links),
        "internal_pair_count": profile_count * (profile_count - 1) // 2,
        "profile_count": profile_count,
        "comparison_count": len(component_comparisons),
        "composite_coherence": evidence["relation_similarity_avg"] or evidence["base_link_similarity_avg"],
        "evidence": evidence,
        "mergeEvidence": {},
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


def _build_profile_component_composites(
        links: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build connected composites without enumerating recursive cliques."""
    links_by_id = {link.get("@id"): link for link in links if link.get("@id")}
    parent = {link_id: link_id for link_id in links_by_id}
    profile_owner = {}

    def find(link_id):
        while parent[link_id] != link_id:
            parent[link_id] = parent[parent[link_id]]
            link_id = parent[link_id]
        return link_id

    def union(left, right):
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for link in links:
        link_id = link.get("@id")
        if not link_id:
            continue
        for profile in _profile_entries(link):
            profile_key = _profile_key(profile)
            if not profile_key:
                continue
            previous = profile_owner.get(profile_key)
            if previous:
                union(link_id, previous)
            else:
                profile_owner[profile_key] = link_id

    groups = {}
    for link_id in links_by_id:
        groups.setdefault(find(link_id), set()).add(link_id)
    return [
        _component_composite(component_link_ids, links_by_id, [])
        for component_link_ids in groups.values()
    ]


def _component_level_item(composite: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "@type": "HierarchyLevelItem",
        "item_type": "CompositeDataLinkingElement",
        "id": composite.get("@id"),
        "label": f"Cluster of {composite.get('profile_count', 0)} profile(s)",
        "similarity": composite.get("composite_coherence", 0.0),
        "status": composite.get("status", "red"),
        "linkRefs": composite.get("linkRefs", []),
        "comparisonRefs": composite.get("comparisonRefs", []),
        "profiles": composite.get("profiles", []),
        "evidence": composite.get("evidence", {}),
        "mergeEvidence": composite.get("mergeEvidence", {}),
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
        base_threshold: Optional[float] = None,
        profile_chunks: Optional[Dict[str, List[str]]] = None,
        profile_labels: Optional[Dict[str, str]] = None,
        profile_chunk_cache: Optional[Dict[str, Any]] = None,
        latent_relation_threshold: float = LATENT_RELATION_THRESHOLD,
        latent_profile_pair_threshold: float = LATENT_PROFILE_PAIR_THRESHOLD,
        latent_min_strong_profile_pairs: int = LATENT_MIN_STRONG_PROFILE_PAIRS,
        latent_min_profile_coverage: float = LATENT_MIN_PROFILE_COVERAGE,
        original_chunk_cache: Optional[Dict[tuple, Any]] = None,
        force_refresh: bool = False,
) -> Dict[str, Any]:
    selected_links = links[:top_n] if top_n and top_n > 0 else links
    # Composite scoring reuses the metrics already stored on every base link.
    # Do not compare every pair of base links again here: with 15 base links
    # that would create 105 unnecessary semantic comparisons.
    comparisons = []
    profile_chunk_cache = profile_chunk_cache if profile_chunk_cache is not None else {}
    original_chunk_cache = original_chunk_cache if original_chunk_cache is not None else {}
    if force_refresh:
        profile_chunk_cache.clear()
        original_chunk_cache.clear()
    if len(selected_links) > MAX_RECURSIVE_BASE_LINKS:
        # A dense 100-profile graph has 4,950 base links and an exponential
        # number of recursive cliques. Connected components preserve the useful
        # grouping while keeping the build bounded.
        effective_base_threshold = base_threshold if base_threshold is not None else relation_threshold
        scalable_links = []
        for link in selected_links:
            try:
                combined_similarity = float((link.get("metrics") or {}).get("combined_similarity", 0)) / 100.0
            except (TypeError, ValueError):
                combined_similarity = 0.0
            if combined_similarity >= effective_base_threshold:
                scalable_links.append(link)
        components = _build_profile_component_composites(scalable_links)
        for component in components:
            component["status"] = (
                "green"
                if float(component.get("composite_coherence") or 0.0) >= relation_threshold
                else "red"
            )
        _attach_terminal_chunk_evidence(
            components,
            profile_chunks,
            profile_labels,
            profile_chunk_cache,
        )
        component_levels = _build_component_levels(selected_links, components)
        green_components = [component for component in components if component.get("status") == "green"]
        return {
            "comparisons": comparisons,
            "recursive_comparisons": [],
            "recursive_levels": component_levels,
            "connected_component_composites": components,
            "connected_component_levels": component_levels,
            "construction": "scalable_connected_components",
            "latent_semantic_bridge_config": {
                "enabled": True,
                "relation_threshold": max(float(relation_threshold), float(latent_relation_threshold)),
                "profile_pair_threshold": float(latent_profile_pair_threshold),
                "minimum_strong_profile_pairs": max(1, int(latent_min_strong_profile_pairs)),
                "minimum_profile_coverage": float(latent_min_profile_coverage),
                "chunk_weight": RECURSIVE_CHUNK_WEIGHT,
                "keyword_weight": RECURSIVE_KEYWORD_WEIGHT,
            },
            "composites": green_components,
            "final_composites": green_components,
            "levels": component_levels,
        }

    recursive_levels, recursive_comparisons = _build_hierarchy_levels(
        selected_links,
        comparisons,
        relation_threshold,
        base_threshold,
        profile_chunks,
        profile_labels,
        profile_chunk_cache,
        latent_relation_threshold,
        latent_profile_pair_threshold,
        latent_min_strong_profile_pairs,
        latent_min_profile_coverage,
        original_chunk_cache,
    )
    terminal_items = _final_composite_items(recursive_levels)
    _attach_terminal_chunk_evidence(
        terminal_items,
        profile_chunks,
        profile_labels,
        profile_chunk_cache,
    )
    recursive_components = [_composite_from_level_item(item) for item in terminal_items]
    component_composites = _build_component_composites(selected_links, comparisons)
    component_levels = _build_component_levels(selected_links, component_composites)
    # Only terminal items from the level-by-level hierarchy are composites.
    # Connected components remain diagnostic data and are never promoted to
    # final output because they bypass the green-parent rule.
    components = recursive_components
    levels = recursive_levels
    construction = "recursive_green_parent_hierarchy"
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
        "final_composites": components,
        "latent_semantic_bridge_config": {
            "enabled": True,
            "relation_threshold": max(float(relation_threshold), float(latent_relation_threshold)),
            "profile_pair_threshold": float(latent_profile_pair_threshold),
            "minimum_strong_profile_pairs": max(1, int(latent_min_strong_profile_pairs)),
            "minimum_profile_coverage": float(latent_min_profile_coverage),
            "chunk_weight": RECURSIVE_CHUNK_WEIGHT,
            "keyword_weight": RECURSIVE_KEYWORD_WEIGHT,
        },
        "composites": components,
        "levels": levels,
    }
