# dl/flask_app.py
import io
import json
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote_plus

from flask import Flask, abort, render_template, request, send_file, make_response, url_for

from dl.similarity import compute_similarities, build_description_top_chunks_for_pair
from dl.reports import build_croissant_report
from dl.link_comparison import compare_datalinkingbase_links, build_composite_datalinking_elements
from dl.refine import refine_similarity, build_refinement_profile
from dl.utils import get_weights_and_threshold

from dl.refine import (
    analyze_distribution, infer_content_type,
    extract_txt_documents, extract_csv_tables_with_samples,
    compare_txt_files, compare_csv_schemas_with_samples
)
# Importa le utility per scaricare i dati
from dl.similarity import get_access_token, fetch_dataset_list, fix_encoding, DETAIL_URL
import requests

# ---------------------------------------------------------------------------- #
# App setup
# ---------------------------------------------------------------------------- #
BASE_DIR = Path(__file__).resolve().parent.parent
COMPOSITE_DOWNLOAD_DIR = Path(tempfile.gettempdir()) / "dataset-linking-composite-downloads"
COMPOSITE_DOWNLOAD_TTL_SECONDS = 6 * 60 * 60
ANALYSIS_CACHE = {}
COMPOSITE_CHUNK_CACHES = {}
app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static")
)


# ---------------------------------------------------------------------------- #
# Utility
# ---------------------------------------------------------------------------- #
def get_requested_folder():
    raw = request.args.get("folder", "")
    raw = unquote_plus(raw).strip()
    if not raw:
        return ""
    return str(Path(raw))


def get_keyword_method():
    method = request.args.get("kw_method", "jaccard").strip().lower()
    return method if method in {"jaccard", "sbert"} else "jaccard"


def _force_refresh_requested():
    value = request.args.get("force_refresh", request.args.get("from_scratch", "false"))
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _local_source_marker(folder_path):
    if not folder_path:
        return "api"
    folder = Path(folder_path)
    try:
        return tuple(
            (path.name, path.stat().st_mtime_ns, path.stat().st_size)
            for path in sorted(folder.glob("*.json"))
            if not path.name.lower().startswith("cache_")
        )
    except OSError:
        return str(folder)


def _analysis_cache_key(folder_path, kw, desc, head, th, keyword_method):
    return (
        str(folder_path or "API_Datagems"),
        _local_source_marker(folder_path),
        round(float(kw), 4),
        round(float(desc), 4),
        round(float(head), 4),
        round(float(th), 4),
        keyword_method,
    )


def _remember_analysis(key, similarities, from_cache):
    ANALYSIS_CACHE[key] = {
        "similarities": similarities,
        "from_cache": from_cache,
        "saved_at": time.time(),
    }


def _cached_analysis(key):
    cached = ANALYSIS_CACHE.get(key)
    if cached:
        print(f"[ANALYSIS] In-memory result reused ({len(cached['similarities'])} pair(s))")
    return cached


def _dataset_payload(data):
    if isinstance(data, dict) and isinstance(data.get("dataset"), dict):
        return data["dataset"]
    return data if isinstance(data, dict) else {}


def _load_profile(profile_id, folder_path, use_api, profile_cache):
    if profile_id in profile_cache:
        return profile_cache[profile_id]

    profile = None
    if use_api:
        from dl import similarity

        cache_folder = Path("/s3/cache")
        profile = similarity._load_cached_api_profile(cache_folder, profile_id)
        if profile is not None:
            print(f"[PROFILE] Cache hit: {profile_id}")
        else:
            token = profile_cache.get("__api_token__")
            if not token:
                token = get_access_token()
                profile_cache["__api_token__"] = token
            headers = {'Authorization': f'Bearer {token}', 'accept': 'application/json'}
            response = requests.get(f"{DETAIL_URL}{profile_id}?format=croissant", headers=headers, timeout=25, verify=False)
            response.raise_for_status()
            profile = _dataset_payload(fix_encoding(response.json()))
            if profile:
                similarity._save_cached_api_profile(cache_folder, profile_id, profile)
                print(f"[PROFILE] Downloaded and cached: {profile_id}")
    else:
        folder = Path(folder_path)
        for file_path in folder.glob("*.json"):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = _dataset_payload(json.load(f))
                current_id = data.get("@id", file_path.name)
                if current_id == profile_id:
                    profile = data
                    break
            except Exception:
                continue

    if profile:
        profile_cache[profile_id] = profile
    return profile


def _build_refinement_evidence_for_link(link, folder_path, use_api, profile_cache):
    dp1 = _load_profile(link.get("dataprofile1ref"), folder_path, use_api, profile_cache)
    dp2 = _load_profile(link.get("dataprofile2ref"), folder_path, use_api, profile_cache)
    if not dp1 or not dp2:
        return {}

    profile_evidence1 = _profile_evidence(dp1)
    profile_evidence2 = _profile_evidence(dp2)

    return {
        "resource_names": sorted(set(profile_evidence1["resource_names"] + profile_evidence2["resource_names"])),
        "column_names": sorted(set(profile_evidence1["column_names"] + profile_evidence2["column_names"])),
    }


def _profile_evidence(profile):
    resource_names = []
    column_names = []
    samples = set()

    for distribution in profile.get("distribution", []) or []:
        if not isinstance(distribution, dict):
            continue
        name = str(distribution.get("name") or "").strip()
        if name:
            resource_names.append(name)

    for record_set in profile.get("recordSet", []) or []:
        if not isinstance(record_set, dict):
            continue
        record_set_name = str(record_set.get("name") or "").strip()
        if record_set_name:
            resource_names.append(record_set_name)
        for field in record_set.get("field", []) or []:
            if not isinstance(field, dict):
                continue
            column_name = str(field.get("name") or field.get("column") or "").strip()
            if column_name:
                column_names.append(column_name)
            for sample in field.get("sample", []) or []:
                if str(sample).strip():
                    samples.add(str(sample).strip())
        if record_set.get("examples"):
            try:
                examples = json.loads(record_set["examples"])
                example = examples[0] if isinstance(examples, list) and examples else examples
                if isinstance(example, dict):
                    for column_name in example:
                        if str(column_name).strip():
                            column_names.append(str(column_name).strip())
            except (TypeError, ValueError, json.JSONDecodeError):
                pass

    return {
        "files": sorted(set(resource_names)),
        "columns": sorted(set(column_names)),
        "resource_names": sorted(set(resource_names)),
        "column_names": sorted(set(column_names)),
        "samples": sorted(samples),
    }


def _attach_profile_evidence(link, folder_path, use_api, profile_cache):
    evidence = {}
    for profile_id in [link.get("dataprofile1ref"), link.get("dataprofile2ref")]:
        profile = _load_profile(profile_id, folder_path, use_api, profile_cache)
        if profile:
            evidence[profile_id] = _profile_evidence(profile)
    link["profile_evidence"] = evidence


def _attach_description_chunks(
        link,
        folder_path,
        use_api,
        profile_cache=None,
        include_original_chunks=False,
):
    if profile_cache is not None:
        if link.get("description_top_chunks") and not include_original_chunks:
            return
        dp1 = _load_profile(link.get("dataprofile1ref"), folder_path, use_api, profile_cache)
        dp2 = _load_profile(link.get("dataprofile2ref"), folder_path, use_api, profile_cache)
        if dp1 and dp2:
            from dl import similarity

            similarity._ensure_models()
            if not link.get("description_top_chunks"):
                link["description_top_chunks"] = similarity._top_description_chunks(
                    dp1.get("description", ""),
                    dp2.get("description", "")
                )
            if include_original_chunks:
                link["_description_chunks_by_profile"] = {
                    str(link.get("dataprofile1ref") or ""): {
                        "id": link.get("dataprofile1ref") or "",
                        "name": link.get("dp1Name") or dp1.get("name") or "",
                        "chunks": similarity._split_chunks(dp1.get("description", "")),
                    },
                    str(link.get("dataprofile2ref") or ""): {
                        "id": link.get("dataprofile2ref") or "",
                        "name": link.get("dp2Name") or dp2.get("name") or "",
                        "chunks": similarity._split_chunks(dp2.get("description", "")),
                    },
                }
            return
    if link.get("description_top_chunks"):
        return
    link["description_top_chunks"] = build_description_top_chunks_for_pair(
        folder_path,
        link.get("dataprofile1ref"),
        link.get("dataprofile2ref"),
        use_api=use_api
    )


def _download_url_for_current_request():
    query_string = request.query_string.decode("utf-8")
    separator = "&" if query_string else ""
    return f"{request.path}?{query_string}{separator}format=json"


def _compact_text(value, limit=1000):
    text = str(value or "").strip()
    return text if len(text) <= limit else f"{text[:limit]}..."


def _score_summary(items, key):
    values = []
    for item in items:
        try:
            values.append(float(item.get(key)))
        except (TypeError, ValueError):
            continue
    return {
        "count": len(items),
        "scored_count": len(values),
        "minimum": round(min(values), 4) if values else 0.0,
        "maximum": round(max(values), 4) if values else 0.0,
        "average": round(sum(values) / len(values), 4) if values else 0.0,
    }


def _compact_chunk_match(match):
    atomic = (match.get("atomic_cross_profile_matches") or [{}])[0]
    return {
        "rank": match.get("rank"),
        "similarity": match.get("similarity", 0.0),
        "left_profile": match.get("left_profile_key") or atomic.get("left_profile_key", ""),
        "right_profile": match.get("right_profile_key") or atomic.get("right_profile_key", ""),
        "left_text": _compact_text(
            match.get("chunk_evidence_1")
            or match.get("chunk1")
            or atomic.get("left_text")
        ),
        "right_text": _compact_text(
            match.get("chunk_evidence_2")
            or match.get("chunk2")
            or atomic.get("right_text")
        ),
        "evaluated_chunk_pair_count": (
            match.get("evaluated_chunk_pair_count")
            or match.get("valid_atomic_pair_count")
            or 0
        ),
    }


def _compact_multi_profile_match(match):
    return {
        "rank": match.get("rank"),
        "similarity": match.get("similarity", 0.0),
        "profiles": [
            {
                "profile": item.get("profile"),
                "profile_id": item.get("profile_id"),
                "text": _compact_text(item.get("text")),
                "similarity": item.get("similarity", 0.0),
            }
            for item in (match.get("profiles") or [])
        ],
    }


def _compact_recursive_comparison(comparison):
    evidence = comparison.get("evidence") or {}
    latent = evidence.get("latent_semantic_bridge") or {}
    result = {
        "id": comparison.get("@id"),
        "recursive_level": comparison.get("recursive_level"),
        "left_ref": comparison.get("leftRef"),
        "right_ref": comparison.get("rightRef"),
        "merge_type": comparison.get("merge_type"),
        "merge_similarity": comparison.get("merge_similarity", comparison.get("relation_similarity", 0.0)),
        "chunk_similarity": comparison.get("chunk_similarity", 0.0),
        "keyword_similarity": comparison.get("keyword_similarity", 0.0),
        "applied_threshold": comparison.get("applied_threshold"),
        "shared_parent_profiles": comparison.get("sharedParentProfiles", []),
        "shared_keywords": evidence.get("shared_keywords", []),
        "chunk_evidence_strategy": evidence.get("chunk_evidence_strategy"),
        "evaluated_profile_pair_count": evidence.get("evaluated_profile_pair_count", 0),
        "top_chunk_matches": [
            _compact_chunk_match(match)
            for match in (evidence.get("description_chunk_matches") or [])[:3]
        ],
        "multi_profile_chunk_matches": [
            _compact_multi_profile_match(match)
            for match in (evidence.get("multi_profile_chunk_matches") or [])[:3]
        ],
    }
    if latent:
        result["latent_bridge"] = {
            "possible_profile_pair_count": latent.get("possible_profile_pair_count", 0),
            "strong_profile_pair_count": latent.get("strong_profile_pair_count", 0),
            "profile_pair_coverage": latent.get("profile_pair_coverage", 0.0),
            "strong_profile_pair_similarity_avg": latent.get("strong_profile_pair_similarity_avg", 0.0),
            "profile_pair_threshold": latent.get("profile_pair_threshold"),
            "minimum_profile_coverage": latent.get("minimum_profile_coverage"),
        }
    return result


def _compact_composite(composite, supporting_limit=10):
    evidence = composite.get("evidence") or {}
    merge_evidence = composite.get("mergeEvidence") or {}
    supporting = sorted(
        composite.get("supportingComparisons", []) or [],
        key=lambda item: item.get("merge_similarity", item.get("relation_similarity", 0.0)),
        reverse=True,
    )
    keywords = composite.get("representative_keywords") or evidence.get("shared_keywords", []) or []
    files = evidence.get("file_table_names", []) or []
    columns = evidence.get("csv_table_column_names", []) or []
    return {
        "id": composite.get("@id"),
        "profiles": composite.get("profiles", []),
        "profile_count": composite.get("profile_count", len(composite.get("profiles", []) or [])),
        "internal_link_count": composite.get("link_count", len(composite.get("linkRefs", []) or [])),
        "source_link_count": composite.get(
            "source_link_count",
            composite.get("link_count", len(composite.get("linkRefs", []) or [])),
        ),
        "internal_pair_count": composite.get(
            "internal_pair_count",
            (composite.get("profile_count", len(composite.get("profiles", []) or []))
             * (composite.get("profile_count", len(composite.get("profiles", []) or [])) - 1)) // 2,
        ),
        "comparison_count": composite.get("comparison_count", 0),
        "recursive_level": composite.get("recursive_level"),
        "status": composite.get("status", "green"),
        "composite_coherence": composite.get("composite_coherence", 0.0),
        "merge_similarity": composite.get("merge_similarity", 0.0),
        "chunk_similarity": merge_evidence.get("chunk_similarity", 0.0),
        "keyword_similarity": merge_evidence.get("keyword_similarity", 0.0),
        "merge_type": merge_evidence.get("merge_type"),
        "chunk_evidence_strategy": merge_evidence.get("chunk_evidence_strategy"),
        "evaluated_profile_pair_count": merge_evidence.get("evaluated_profile_pair_count", 0),
        "reused_profile_pair_count": merge_evidence.get("reused_profile_pair_count", 0),
        "new_profile_pair_count": merge_evidence.get("new_profile_pair_count", 0),
        "main_keywords": keywords[:100],
        "main_keyword_count": len(keywords),
        "file_table_names": [_compact_text(value, 300) for value in files[:50]],
        "file_table_name_count": len(files),
        "column_names": [_compact_text(value, 300) for value in columns[:50]],
        "column_name_count": len(columns),
        "top_chunk_matches": [
            _compact_chunk_match(match)
            for match in (merge_evidence.get("description_chunk_matches") or [])[:3]
        ],
        "multi_profile_chunk_matches": [
            _compact_multi_profile_match(match)
            for match in (merge_evidence.get("multi_profile_chunk_matches") or [])[:3]
        ],
        "supporting_path_count": len(supporting),
        "supporting_paths_included": min(len(supporting), supporting_limit),
        "supporting_paths": [
            _compact_recursive_comparison(comparison)
            for comparison in supporting[:supporting_limit]
        ],
    }


def _compact_level_summary(level):
    items = level.get("items", []) or []
    similarities = []
    profile_counts = []
    for item in items:
        try:
            similarities.append(float(item.get("similarity")))
        except (TypeError, ValueError):
            pass
        profile_counts.append(len(item.get("profiles", []) or []))
    return {
        "level": level.get("level"),
        "name": level.get("name"),
        "item_count": len(items),
        "profile_count_minimum": min(profile_counts) if profile_counts else 0,
        "profile_count_maximum": max(profile_counts) if profile_counts else 0,
        "similarity_minimum": round(min(similarities), 4) if similarities else 0.0,
        "similarity_maximum": round(max(similarities), 4) if similarities else 0.0,
        "similarity_average": round(sum(similarities) / len(similarities), 4) if similarities else 0.0,
    }


def _compact_composite_report(report):
    comparisons = report.get("comparisons", []) or []
    recursive = report.get("recursive_comparisons", []) or []
    merge_type_counts = {}
    for comparison in recursive:
        merge_type = comparison.get("merge_type") or "unknown"
        merge_type_counts[merge_type] = merge_type_counts.get(merge_type, 0) + 1

    return {
        "@context": report.get("@context"),
        "@type": report.get("@type"),
        "source": report.get("source"),
        "from_cache": report.get("from_cache"),
        "download_projection": {
            "mode": "compact",
            "supporting_paths_per_composite": 10,
            "chunk_matches_per_merge": 3,
            "omitted": [
                "duplicate recursive level payloads",
                "full base and recursive comparison evidence",
                "excluded self-pair lists",
                "complete cross-profile score lists",
            ],
        },
        "selection": report.get("selection", {}),
        "weights": report.get("weights", {}),
        "level_summaries": [_compact_level_summary(level) for level in report.get("levels", []) or []],
        "base_comparison_summary": _score_summary(comparisons, "relation_similarity"),
        "recursive_comparison_summary": {
            **_score_summary(recursive, "merge_similarity"),
            "merge_type_counts": merge_type_counts,
        },
        "connected_component_count": len(report.get("connected_component_composites", []) or []),
        "composite_count": len(report.get("composites", []) or []),
        "composites": [_compact_composite(composite) for composite in report.get("composites", []) or []],
    }


def _cleanup_cached_composite_downloads(now=None):
    if not COMPOSITE_DOWNLOAD_DIR.exists():
        return
    cutoff = float(now if now is not None else time.time()) - COMPOSITE_DOWNLOAD_TTL_SECONDS
    for path in COMPOSITE_DOWNLOAD_DIR.glob("*.json"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
        except OSError:
            continue


def _cache_compact_composite_report(report):
    COMPOSITE_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    _cleanup_cached_composite_downloads()
    token = uuid.uuid4().hex
    target = COMPOSITE_DOWNLOAD_DIR / f"{token}.json"
    temporary = COMPOSITE_DOWNLOAD_DIR / f"{token}.tmp"
    compact = _compact_composite_report(report)
    temporary.write_text(
        json.dumps(compact, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    temporary.replace(target)
    return token


def _cache_full_composite_report(report, token):
    """Store the complete report beside its compact projection."""
    COMPOSITE_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target = COMPOSITE_DOWNLOAD_DIR / f"{token}_full.json"
    temporary = COMPOSITE_DOWNLOAD_DIR / f"{token}_full.tmp"
    temporary.write_text(
        json.dumps(report, ensure_ascii=False, indent=4),
        encoding="utf-8",
    )
    temporary.replace(target)


# ---------------------------------------------------------------------------- #
# Routes
# ---------------------------------------------------------------------------- #


@app.route("/composite_download/<token>")
def download_cached_composite(token):
    if len(token) != 32 or any(character not in "0123456789abcdef" for character in token.lower()):
        abort(404)
    mode = request.args.get("mode", "compact").strip().lower()
    if mode not in {"compact", "full"}:
        abort(404)
    suffix = "_full" if mode == "full" else ""
    path = COMPOSITE_DOWNLOAD_DIR / f"{token.lower()}{suffix}.json"
    if not path.is_file():
        abort(404)

    timestamp = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y%m%d_%H%M%S")
    response = make_response(send_file(
        path,
        mimetype="application/json",
        as_attachment=True,
        download_name=f"composite_links_{mode}_{timestamp}.json",
    ))
    response.set_cookie("downloadComplete", "1", max_age=10)
    return response

@app.route("/")
def index():
    folder_path = get_requested_folder()
    force_refresh = _force_refresh_requested()

    # If there is no folder_path --> API
    use_api = True if not folder_path else False

    kw_weight, desc_weight, head_weight, th, normalized = get_weights_and_threshold()
    keyword_method = get_keyword_method()

    error, similarities, from_cache = compute_similarities(
        folder_path=folder_path if folder_path else None,
        kw_weight=kw_weight,
        desc_weight=desc_weight,
        head_weight=head_weight,
        threshold=th,
        use_api=use_api,
        include_description_chunks=True,
        keyword_method=keyword_method,
        force_refresh=force_refresh,
    )
    if not error:
        _remember_analysis(
            _analysis_cache_key(folder_path, kw_weight, desc_weight, head_weight, th, keyword_method),
            similarities,
            from_cache,
        )

    success = None
    if not error:
        source_name = "☁️ API (Datagems-Dev)" if use_api else f"📂 Local: {folder_path}"
        success_message = (
            f"✅ Analysis completed | Source: {source_name}"
            f" | weights → KW: {kw_weight:.2f}, DESC: {desc_weight:.2f}, HEAD: {head_weight:.2f}"
        )
        if normalized:
            success_message += " ⚖️ Weights normalized"
        if from_cache:
            success_message += " | ⚡ Loaded from cache"

        success_message += f" | keyword method: {keyword_method.upper()} | threshold: {th:.0f}%"
        success = success_message

    return render_template(
        "index.html",
        similarities=similarities,
        folder=folder_path or "",
        kw=kw_weight,
        desc=desc_weight,
        head=head_weight,
        th=th,
        kw_method=keyword_method,
        error=error,
        success=success
    )


@app.route("/save")
def save_results():
    folder_path = get_requested_folder()
    use_api = True if not folder_path else False

    kw_weight, desc_weight, head_weight, th, normalized = get_weights_and_threshold()
    keyword_method = get_keyword_method()
    force_refresh = _force_refresh_requested()

    error, similarities, _ = compute_similarities(
        folder_path=folder_path if folder_path else None,
        kw_weight=kw_weight,
        desc_weight=desc_weight,
        head_weight=head_weight,
        threshold=th,
        use_api=use_api,
        keyword_method=keyword_method,
        force_refresh=force_refresh,
    )

    if error:
        return f"❌ Cannot save results: {error}", 400

    # --- Fetch metadata for report ---
    file_data = {}
    if not use_api:
        # From local files
        folder_display = folder_path or (Path.home() / "Desktop" / "Profiles")
        for file in Path(folder_display).glob("*.json"):
            try:
                with open(file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    file_data[file.name] = {
                        "keywords": sorted(set(data.get("keywords", []))),
                        "description": data.get("description", ""),
                        "headline": data.get("headline", ""),
                        "id": data.get("@id", "")
                    }
            except Exception:
                continue

    weights = {
        "keywords": kw_weight, "description": desc_weight,
        "headline": head_weight, "normalized": normalized,
        "keyword_similarity_method": keyword_method
    }

    source_label = folder_path if folder_path else "API_Datagems"
    output_data = build_croissant_report(source_label, weights, similarities, file_data)

    buffer = io.BytesIO()
    json_bytes = json.dumps(output_data, ensure_ascii=False, indent=4).encode("utf-8")
    buffer.write(json_bytes)
    buffer.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"similarity_{timestamp}.json"

    response = make_response(send_file(
        buffer, mimetype="application/json", as_attachment=True, download_name=filename
    ))
    response.set_cookie("downloadComplete", "1", max_age=10)
    return response


@app.route("/compare_links")
def compare_links():
    folder_path = get_requested_folder()
    use_api = True if not folder_path else False

    kw_weight, desc_weight, head_weight, th, normalized = get_weights_and_threshold()
    keyword_method = get_keyword_method()
    force_refresh = _force_refresh_requested()
    relation_threshold = request.args.get("relation_th", 0.0)
    top_n = request.args.get("top_n", "").strip()
    selected_pairs_raw = request.args.get("selected_pairs", "").strip()
    output_format = request.args.get("format", "html").strip().lower()

    try:
        relation_threshold = float(relation_threshold)
    except (TypeError, ValueError):
        relation_threshold = 0.0

    try:
        top_n_value = int(top_n) if top_n else None
    except ValueError:
        top_n_value = None

    selected_pairs = []
    if selected_pairs_raw:
        try:
            loaded_pairs = json.loads(selected_pairs_raw)
            if isinstance(loaded_pairs, list):
                selected_pairs = [
                    tuple(pair)
                    for pair in loaded_pairs
                    if isinstance(pair, list) and len(pair) == 2
                ]
        except json.JSONDecodeError:
            selected_pairs = []

    error, similarities, from_cache = compute_similarities(
        folder_path=folder_path if folder_path else None,
        kw_weight=kw_weight,
        desc_weight=desc_weight,
        head_weight=head_weight,
        threshold=th,
        use_api=use_api,
        keyword_method=keyword_method,
        force_refresh=force_refresh,
    )

    if error:
        return f"❌ Cannot compare links: {error}", 400

    above_threshold = [s for s in similarities if s.get("passes_threshold")]
    if selected_pairs:
        selected_pair_keys = {frozenset(pair) for pair in selected_pairs}
        above_threshold = [
            s for s in above_threshold
            if frozenset([str(s.get("id1")), str(s.get("id2"))]) in selected_pair_keys
        ]
    weights = {
        "keywords": kw_weight,
        "description": desc_weight,
        "headline": head_weight,
        "normalized": normalized,
        "threshold": th,
        "keyword_similarity_method": keyword_method,
    }

    source_label = folder_path if folder_path else "API_Datagems"
    base_report = build_croissant_report(source_label, weights, above_threshold, {})
    profile_cache = {}
    for link in base_report["links"]:
        _attach_profile_evidence(
            link,
            folder_path if folder_path else None,
            use_api,
            profile_cache
        )
        _attach_description_chunks(
            link,
            folder_path if folder_path else None,
            use_api,
            profile_cache,
        )
        link["refinement_evidence"] = _build_refinement_evidence_for_link(
            link,
            folder_path if folder_path else None,
            use_api,
            profile_cache
        )

    comparisons = compare_datalinkingbase_links(
        base_report["links"],
        relation_threshold=relation_threshold,
        top_n=top_n_value
    )

    selection = {
        "mode": "selected_links" if selected_pairs else "all_above_threshold",
        "minimum_combined_similarity": th,
        "links_considered": len(base_report["links"]),
    }
    selection["includes_refinement_evidence"] = True
    selection["includes_description_chunks"] = True
    if selected_pairs:
        selection["selected_pairs"] = len(selected_pairs)
    if top_n_value:
        selection["top_n"] = top_n_value
    if relation_threshold > 0:
        selection["minimum_relation_similarity"] = relation_threshold

    output_data = {
        "@context": "http://mlcommons.org/croissant/",
        "@type": "DataLinkingBaseComparisonReport",
        "source": source_label,
        "from_cache": from_cache,
        "selection": selection,
        "weights": weights,
        "comparisons": comparisons,
    }

    if output_format != "json":
        return render_template(
            "link_comparisons.html",
            report=output_data,
            comparisons=comparisons,
            download_url=_download_url_for_current_request(),
            folder=folder_path or "",
        )

    buffer = io.BytesIO()
    buffer.write(json.dumps(output_data, indent=4, ensure_ascii=False).encode("utf-8"))
    buffer.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"link_comparisons_{timestamp}.json"
    response = make_response(send_file(
        buffer, mimetype="application/json", as_attachment=True, download_name=filename
    ))
    response.set_cookie("downloadComplete", "1", max_age=10)
    return response


@app.route("/composite_links")
def composite_links():
    folder_path = get_requested_folder()
    use_api = True if not folder_path else False
    force_refresh = _force_refresh_requested()

    kw_weight, desc_weight, head_weight, th, normalized = get_weights_and_threshold()
    keyword_method = get_keyword_method()
    relation_threshold = request.args.get("relation_th", 0.5)
    top_n = request.args.get("top_n", "").strip()
    output_format = request.args.get("format", "html").strip().lower()

    try:
        relation_threshold = float(relation_threshold)
    except (TypeError, ValueError):
        relation_threshold = 0.5

    try:
        top_n_value = int(top_n) if top_n else None
    except ValueError:
        top_n_value = None

    started_at = time.perf_counter()
    cache_key = _analysis_cache_key(folder_path, kw_weight, desc_weight, head_weight, th, keyword_method)
    cached = None if force_refresh else _cached_analysis(cache_key)
    if cached:
        error = None
        similarities = cached["similarities"]
        from_cache = True
        print("[COMPOSITE] Reusing analysis from the home page")
    else:
        print("[COMPOSITE] No in-memory analysis found; loading similarity cache or computing")
        error, similarities, from_cache = compute_similarities(
            folder_path=folder_path if folder_path else None,
            kw_weight=kw_weight,
            desc_weight=desc_weight,
            head_weight=head_weight,
            threshold=th,
            use_api=use_api,
            include_description_chunks=True,
            keyword_method=keyword_method,
            force_refresh=force_refresh,
        )
        if not error:
            _remember_analysis(cache_key, similarities, from_cache)

    if error:
        return f"❌ Cannot build composite links: {error}", 400

    weights = {
        "keywords": kw_weight,
        "description": desc_weight,
        "headline": head_weight,
        "normalized": normalized,
        "threshold": th,
        "keyword_similarity_method": keyword_method,
    }

    source_label = folder_path if folder_path else "API_Datagems"
    # Keep every base pair. Green pairs can build the next level, while red
    # pairs remain visible and can mark a composite as weak.
    base_report = build_croissant_report(source_label, weights, similarities, {})
    print(f"[COMPOSITE] Base pairs: {len(base_report['links'])}")
    profile_cache = {}
    for link in base_report["links"]:
        _attach_profile_evidence(
            link,
            folder_path if folder_path else None,
            use_api,
            profile_cache
        )
        _attach_description_chunks(
            link,
            folder_path if folder_path else None,
            use_api,
            profile_cache,
            include_original_chunks=False,
        )
        link["refinement_evidence"] = _build_refinement_evidence_for_link(
            link,
            folder_path if folder_path else None,
            use_api,
            profile_cache
        )
    print(f"[COMPOSITE] Profile evidence ready for {len(profile_cache) - (1 if '__api_token__' in profile_cache else 0)} profile(s)")
    from dl import similarity as similarity_module
    profile_chunks = {
        str(profile_id).strip().lower(): similarity_module._split_chunks(profile.get("description", ""))
        for profile_id, profile in profile_cache.items()
        if profile_id != "__api_token__" and isinstance(profile, dict)
    }
    profile_labels = {
        str(profile_id).strip().lower(): str(profile.get("name") or profile_id)
        for profile_id, profile in profile_cache.items()
        if profile_id != "__api_token__" and isinstance(profile, dict)
    }

    composite_cache = COMPOSITE_CHUNK_CACHES.setdefault(
        folder_path or "API_Datagems",
        {"profile": {}, "original": {}},
    )
    composite_data = build_composite_datalinking_elements(
        base_report["links"],
        relation_threshold=relation_threshold,
        top_n=top_n_value,
        base_threshold=th / 100.0,
        profile_chunks=profile_chunks,
        profile_labels=profile_labels,
        profile_chunk_cache=composite_cache["profile"],
        original_chunk_cache=composite_cache["original"],
        force_refresh=force_refresh,
    )
    for level in composite_data.get("levels", []):
        items = level.get("items", []) or []
        green = sum(1 for item in items if item.get("status") == "green")
        red = sum(1 for item in items if item.get("status") == "red")
        print(f"[COMPOSITE] Level {level.get('level')}: {len(items)} item(s), {green} green, {red} red")
    print(
        f"[COMPOSITE] Final terminal composites: {len(composite_data.get('composites', []))} "
        f"in {time.perf_counter() - started_at:.2f}s"
    )
    selection = {
        "mode": "all_pairs_with_green_parent_merges",
        "minimum_combined_similarity": th,
        "minimum_relation_similarity": relation_threshold,
        "links_considered": len(base_report["links"]),
        "green_base_links": sum(
            1 for link in base_report["links"]
            if float((link.get("metrics") or {}).get("combined_similarity") or 0) / 100.0 >= th / 100.0
        ),
        "comparisons_used": len(composite_data["comparisons"]),
        "recursive_comparisons_used": len(composite_data.get("recursive_comparisons", [])),
        "final_composite_count": len(composite_data.get("final_composites", [])),
        "includes_description_chunks": True,
        "composite_construction": composite_data.get("construction"),
    }
    if top_n_value:
        selection["top_n"] = top_n_value

    output_data = {
        "@context": "http://mlcommons.org/croissant/",
        "@type": "CompositeDataLinkingElementReport",
        "source": source_label,
        "from_cache": from_cache,
        "selection": selection,
        "weights": weights,
        "levels": composite_data["levels"],
        "recursive_levels": composite_data.get("recursive_levels", []),
        "connected_component_levels": composite_data.get("connected_component_levels", []),
        "composites": composite_data["composites"],
        "final_composites": composite_data.get("final_composites", composite_data["composites"]),
        "connected_component_composites": composite_data.get("connected_component_composites", []),
        "comparisons": composite_data["comparisons"],
        "recursive_comparisons": composite_data.get("recursive_comparisons", []),
    }

    if output_format != "json":
        download_token = _cache_compact_composite_report(output_data)
        _cache_full_composite_report(output_data, download_token)
        return render_template(
            "composite_links.html",
            report=output_data,
            levels=output_data["levels"],
            composites=output_data["composites"],
            download_url=url_for("download_cached_composite", token=download_token),
            full_download_url=url_for("download_cached_composite", token=download_token, mode="full"),
            folder=folder_path or "",
        )

    compact_download = request.args.get("download_detail", "compact").strip().lower() != "full"
    download_data = _compact_composite_report(output_data) if compact_download else output_data
    buffer = io.BytesIO()
    if compact_download:
        serialized = json.dumps(download_data, ensure_ascii=False, separators=(",", ":"))
    else:
        serialized = json.dumps(download_data, indent=4, ensure_ascii=False)
    buffer.write(serialized.encode("utf-8"))
    buffer.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "compact" if compact_download else "full"
    filename = f"composite_links_{suffix}_{timestamp}.json"
    response = make_response(send_file(
        buffer, mimetype="application/json", as_attachment=True, download_name=filename
    ))
    response.set_cookie("downloadComplete", "1", max_age=10)
    return response


@app.route("/save_single")
def save_single():
    dataprofile1 = request.args.get("d1")
    dataprofile2 = request.args.get("d2")
    folder_path = request.args.get("folder")
    use_api = True if not folder_path else False

    kw, desc, head, th, _ = get_weights_and_threshold()
    keyword_method = get_keyword_method()

    error, similarities, _ = compute_similarities(
        folder_path if folder_path else None,
        kw_weight=kw,
        desc_weight=desc,
        head_weight=head,
        threshold=th,
        use_api=use_api,
        keyword_method=keyword_method
    )

    if error:
        return f"❌ Cannot save results: {error}", 400

    match = next(
        (s for s in similarities if (s["dataprofile1"] == dataprofile1 and s["dataprofile2"] == dataprofile2)
         or (s["dataprofile1"] == dataprofile2 and s["dataprofile2"] == dataprofile1)),
        None
    )

    if not match:
        return f"❌ Pair {dataprofile1} / {dataprofile2} not found.", 404

    description_top_chunks = match.get("description_top_chunks") or build_description_top_chunks_for_pair(
        folder_path if folder_path else None,
        match.get("id1"),
        match.get("id2"),
        use_api=use_api
    )

    output_data = {
        "@context": "http://mlcommons.org/croissant/",
        "@type": "DatasetSimilarityReport",
        "source": folder_path if folder_path else "API_Datagems",
        "analysis_configuration": {
            "weights": {
                "keywords": kw,
                "description": desc,
                "headline": head
            },
            "threshold": th,
            "keyword_similarity_method": keyword_method
        },
        "link": [{
            "@type": "DataLinkingBase",
            "@id": f"link:{uuid.uuid4()}",
            "dp1Name": match['dataprofile1'],
            "dp2Name": match['dataprofile2'],
            "dataprofile1ref": match.get("id1"),
            "dataprofile2ref": match.get("id2"),
            "metrics": {
                "keywords_similarity": match["keywords_similarity"],
                "keyword_similarity_method": match.get("keyword_similarity_method", keyword_method),
                "description_similarity": match["description_similarity"],
                "headline_similarity": match["headline_similarity"],
                "combined_similarity": match["combined_similarity"],
                "headline_used_in_score": match.get("headline_used_in_score", True),
                "field_usage": match.get("field_usage"),
                "effective_weights": match.get("effective_weights")
            },
            "common_keywords": [kw.strip() for kw in match["common_keywords"].split(",") if kw.strip()],
            "unique_to_1": [kw.strip() for kw in match.get("unique_to_1", "").split(",") if kw.strip()],
            "unique_to_2": [kw.strip() for kw in match.get("unique_to_2", "").split(",") if kw.strip()],
            "description_top_chunks": description_top_chunks
        }]
    }

    buffer = io.BytesIO()
    # Keep non-ASCII metadata readable in downloaded JSON.
    buffer.write(json.dumps(output_data, indent=4, ensure_ascii=False).encode("utf-8"))
    buffer.seek(0)

    filename = f"match_{dataprofile1}_{dataprofile2}.json"
    return send_file(buffer, mimetype="application/json", as_attachment=True, download_name=filename)


@app.route("/refine")
def refine_pair():
    import json as _json
    import sys
    import os
    from dl.refine import (
        infer_content_type, extract_txt_documents, extract_csv_tables_with_samples,
        compare_txt_files, compare_csv_schemas_with_samples, build_graph_json
    )

    id1 = request.args.get("id1")
    id2 = request.args.get("id2")
    folder_path = request.args.get("folder", "").strip()

    dataprofile1_name = request.args.get("d1", "")
    dataprofile2_name = request.args.get("d2", "")

    kw_sim = request.args.get("kw_s", 0)
    desc_sim = request.args.get("desc_s", 0)
    head_sim = request.args.get("head_s", 0)
    comb_sim = request.args.get("comb_s", 0)

    dp1, dp2 = None, None

    try:
        if folder_path:
            # A) LOCAL
            print(f"📂 Refining in local mode from: {folder_path}")
            folder = Path(folder_path)
            for file_path in folder.glob("*.json"):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = _json.load(f)
                        # Verifichiamo se l'ID del file o l'ID interno coincide
                        current_id = data.get("@id", file_path.name)
                        if current_id == id1:
                            dp1 = data
                        if current_id == id2:
                            dp2 = data
                except:
                    continue

            if not dp1 or not dp2:
                return f"❌ Impossibile trovare i file locali per gli ID: {id1} o {id2} nella cartella {folder_path}"

        else:
            # B) API
            print(f"☁️ Refining in API mode for IDs: {id1}, {id2}")
            token = get_access_token()
            headers = {'Authorization': f'Bearer {token}', 'accept': 'application/json'}

            # Usiamo verify=False per bypassare il certificato scaduto
            res1 = requests.get(f"{DETAIL_URL}{id1}?format=croissant", headers=headers, timeout=20, verify=False)
            res2 = requests.get(f"{DETAIL_URL}{id2}?format=croissant", headers=headers, timeout=20, verify=False)

            res1.raise_for_status()
            res2.raise_for_status()

            dp1 = fix_encoding(res1.json()).get('dataset', {})
            dp2 = fix_encoding(res2.json()).get('dataset', {})

        # ---------------------------------------------------------
        # From here same strucure Local/Api
        # ---------------------------------------------------------
        content_type1 = infer_content_type(dp1)
        content_type2 = infer_content_type(dp2)

        txt1, txt2 = extract_txt_documents(dp1), extract_txt_documents(dp2)
        csv1, csv2 = extract_csv_tables_with_samples(dp1), extract_csv_tables_with_samples(dp2)

        txt_cmp = compare_txt_files(txt1, txt2)
        csv_cmp = compare_csv_schemas_with_samples(csv1, csv2)

        kw_w = float(request.args.get("kw_w", 0.6))
        desc_w = float(request.args.get("desc_w", 0.3))
        head_w = float(request.args.get("head_w", 0.1))

        # 5. Report for graph
        report = {
            "dataprofile1": dataprofile1_name,
            "dataprofile2": dataprofile2_name,
            "dataprofile1ref": id1,
            "dataprofile2ref": id2,
            "kw_sim": kw_sim,
            "desc_sim": desc_sim,
            "head_sim": head_sim,
            "combined_similarity": comb_sim,
            "kw_w": kw_w,
            "desc_w": desc_w,
            "head_w": head_w,
            "txt_comparison": txt_cmp,
            "csv_comparison": csv_cmp,
            "note": f"Dataset 1: {content_type1} | Dataset 2: {content_type2}. Match: {len(txt_cmp['common_document_names'])} file."
        }

        # 6. Generation of KG
        graph = build_graph_json(report, dp1, dp2)

        # Save on Desktop
        desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        file_path = os.path.join(desktop, "grafo_visualizer.json")
        with open(file_path, "w", encoding="utf-8") as f:
            _json.dump(graph, f, indent=4, ensure_ascii=False)

        status_msg = f"✅ Grafo generato sul Desktop: {file_path}"

        # 7. Rendering HTML
        return f"""
            <div style="font-family: sans-serif; padding: 20px;">
                <h2>🔁 Refinement Report: {dataprofile1_name} ↔ {dataprofile2_name}</h2>
                <p style="color: green; font-weight: bold;">{status_msg}</p>
                <hr>
                <p><b>Combined Similarity:</b> {comb_sim}%</p>
                <p><b>Dati Sorgente:</b> {"📁 Locale" if folder_path else "☁️ API"}</p>
                <details>
                    <summary style="cursor:pointer; color:blue;">Visualizza JSON Report completo</summary>
                    <pre style="background:#f4f4f4; padding:15px; border: 1px solid #ddd; margin-top:10px;">{_json.dumps(report, indent=4, ensure_ascii=False)}</pre>
                </details>
                <br>
                <p><a href="/" style="text-decoration:none; padding:10px; background:#007bff; color:white; border-radius:5px;">⬅️ Torna alla lista</a></p>
            </div>
        """

    except Exception as e:
        sys.stderr.write(f"\n❌ ERRORE REFINE: {str(e)}\n")
        return f"❌ Errore durante il refine: {str(e)}"

@app.route("/refine_download")
def refine_download():
    dataprofile1 = request.args.get("d1", "")
    dataprofile2 = request.args.get("d2", "")
    folder_path = request.args.get("folder", "").strip().replace("\\", "/")

    try:
        report = refine_similarity(folder_path if folder_path else None, dataprofile1, dataprofile2)
        profile = build_refinement_profile(report)
        buffer = io.BytesIO()
        buffer.write(json.dumps(profile, indent=4).encode("utf-8"))
        buffer.seek(0)
        return send_file(buffer, mimetype="application/json", as_attachment=True, download_name="refinement.json")
    except Exception as e:
        return f"❌ Error: {e}", 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
