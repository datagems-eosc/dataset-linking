# dl/fastapi_app.py
import io
import json
import logging
import uuid
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from dl.refine import refine_similarity, build_refinement_profile
from dl.reports import build_croissant_report
from dl import similarity as similarity_module
from dl.similarity import compute_similarities, build_description_top_chunks_for_pair
from dl.similarity import get_access_token, fetch_dataset_list, fetch_details_from_list
from dl.dmm_client import register_graph_in_dmm
from dl.utils import normalize_weights
from dl.link_comparison import compare_datalinkingbase_links, build_composite_datalinking_elements

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Profile Similarity API", 
    version="1.0",
    root_path="/dl"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _keyword_method(value: str) -> str:
    method = str(value or "jaccard").strip().lower()
    if method not in {"jaccard", "sbert"}:
        raise HTTPException(status_code=400, detail="keyword_method must be 'jaccard' or 'sbert'.")
    return method


def _json_download(payload: dict, filename: str) -> StreamingResponse:
    data = json.dumps(payload, ensure_ascii=False, indent=4).encode("utf-8")
    buf = io.BytesIO(data)
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(buf, media_type="application/json", headers=headers)

JOBS: Dict[str, Dict[str, Any]] = {}
COMPOSITE_CHUNK_CACHES: Dict[str, Dict[str, Dict[Any, Any]]] = {}


def _normalize_folder_path(folder: str) -> str:
    return (folder or "").strip().replace("\\", "/")


def _profile_filename_lookup(folder: str) -> Dict[str, str]:
    lookup = {}
    for file_path in Path(folder).glob("*.json"):
        lookup[file_path.name] = file_path.name
        lookup[file_path.stem] = file_path.name
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get("dataset"), dict):
                data = data["dataset"]
            if isinstance(data, dict):
                if data.get("@id"):
                    lookup[str(data.get("@id"))] = file_path.name
                if data.get("name"):
                    lookup[str(data.get("name"))] = file_path.name
        except Exception:
            continue
    return lookup


def _load_profiles_for_composites(folder: Optional[str]) -> tuple[Dict[str, Any], Dict[str, str]]:
    """Load descriptions and labels using the same source mode as similarity."""
    profiles: Dict[str, Any] = {}
    if folder:
        for file_path in Path(folder).glob("*.json"):
            if file_path.name.lower().startswith("cache_"):
                continue
            try:
                data = json.loads(file_path.read_text(encoding="utf-8"))
                data = similarity_module._dataset_payload(data)
                profile_id = str(data.get("@id") or file_path.name)
                profiles[profile_id.strip().lower()] = data
            except (OSError, ValueError, TypeError):
                continue
    else:
        token = get_access_token()
        datasets = fetch_dataset_list(token)
        api_profiles = fetch_details_from_list(token, datasets, Path("/s3/cache"))
        for profile_id, data in api_profiles.items():
            profiles[str(profile_id).strip().lower()] = data

    chunks = {
        profile_id: similarity_module._split_chunks(profile.get("description", ""))
        for profile_id, profile in profiles.items()
        if isinstance(profile, dict)
    }
    labels = {
        profile_id: str(profile.get("name") or profile_id)
        for profile_id, profile in profiles.items()
        if isinstance(profile, dict)
    }
    return chunks, labels


def _composite_cache_for(folder: Optional[str]) -> Dict[str, Dict[Any, Any]]:
    key = folder or "API_Datagems"
    return COMPOSITE_CHUNK_CACHES.setdefault(key, {"profile": {}, "original": {}})


def _attach_graphs_to_similarities(
        folder: Optional[str],
        similarities: List[Dict[str, Any]],
        kw: float,
        desc: float,
        head: float,
        th: float,
        only_above_threshold: bool = True,
) -> Dict[str, int]:
    lookup = _profile_filename_lookup(folder) if folder else {}
    generated = 0
    failed = 0
    skipped = 0
    dmm_token = None

    for item in similarities:
        if only_above_threshold and not item.get("passes_threshold"):
            skipped += 1
            continue

        d1 = lookup.get(str(item.get("id1"))) or lookup.get(str(item.get("dataprofile1"))) or str(item.get("id1"))
        d2 = lookup.get(str(item.get("id2"))) or lookup.get(str(item.get("dataprofile2"))) or str(item.get("id2"))

        try:
            report = refine_similarity(
                folder,
                d1,
                d2,
                item.get("keywords_similarity", 0),
                item.get("description_similarity", 0),
                item.get("headline_similarity", 0),
                item.get("combined_similarity", 0),
                kw,
                desc,
                head,
            )
            if isinstance(report, dict) and report.get("error"):
                item["graph_error"] = report["error"]
                failed += 1
                continue
            item["graph"] = report.get("graph")
            if item.get("graph"):
                if dmm_token is None:
                    dmm_token = get_access_token()
                register_graph_in_dmm(item["graph"], token=dmm_token)
            generated += 1
        except Exception as exc:
            item["graph_error"] = str(exc)
            failed += 1

    return {"generated": generated, "failed": failed, "skipped": skipped}


def _run_report_job(
        job_id: str,
        folder: Optional[str],
        kw: float,
        desc: float,
        head: float,
        th: float,
        include_chunks: bool = False,
        include_graphs: bool = False,
        graphs_only_above_threshold: bool = True,
        keyword_method: str = "jaccard",
        force_refresh: bool = False,
) -> None:
    try:
        JOBS[job_id]["status"] = "in_progress"
        JOBS[job_id]["progress"] = 5
        JOBS[job_id]["message"] = "Starting report job..."

        if folder:
            folder = _normalize_folder_path(folder)

        JOBS[job_id]["progress"] = 20
        JOBS[job_id]["message"] = "Computing similarities..."

        use_api = (folder is None)

        error, similarities, from_cache = compute_similarities(
            folder,
            kw,
            desc,
            head,
            threshold=th,
            use_api=use_api,
            include_description_chunks=include_chunks,
            keyword_method=keyword_method,
            force_refresh=force_refresh,
        )
        if error:
            JOBS[job_id]["status"] = "failed"
            JOBS[job_id]["progress"] = 100
            JOBS[job_id]["message"] = error
            return

        logger.info(
            "Dataset Linking analysis completed successfully",
            extra={"job_id": job_id, "similarity_count": len(similarities)},
        )

        graph_summary = None
        if include_graphs:
            JOBS[job_id]["progress"] = 70
            JOBS[job_id]["message"] = "Generating pair graphs..."
            graph_summary = _attach_graphs_to_similarities(
                folder,
                similarities,
                kw,
                desc,
                head,
                th,
                only_above_threshold=graphs_only_above_threshold,
            )

        JOBS[job_id]["progress"] = 85
        JOBS[job_id]["message"] = "Building report..."

        weights = {
            "keywords": kw,
            "description": desc,
            "headline": head,
            "normalized": JOBS[job_id]["params"]["normalized"],
            "threshold": th,
            "include_description_chunks": include_chunks,
            "include_graphs": include_graphs,
            "graphs_only_above_threshold": graphs_only_above_threshold,
            "keyword_similarity_method": keyword_method,
        }

        report = build_croissant_report(folder, weights, similarities)
        report["from_cache"] = from_cache
        report["graphs"] = graph_summary

        JOBS[job_id]["status"] = "completed"
        JOBS[job_id]["progress"] = 100
        JOBS[job_id]["message"] = "Completed"
        JOBS[job_id]["result"] = report

    except Exception as e:
        JOBS[job_id]["status"] = "failed"
        JOBS[job_id]["progress"] = 100
        JOBS[job_id]["message"] = str(e)
        JOBS[job_id]["traceback"] = traceback.format_exc()


def _run_refine_job(job_id: str, folder: str, d1: str, d2: str, kw: float, desc: float, head: float, th: float) -> None:
    try:
        JOBS[job_id]["status"] = "in_progress"
        JOBS[job_id]["progress"] = 5
        JOBS[job_id]["message"] = "Starting refine job..."

        folder = _normalize_folder_path(folder)

        JOBS[job_id]["progress"] = 60
        JOBS[job_id]["message"] = "Running refinement..."

        report = refine_similarity(folder, d1, d2, kw, desc, head, th)
        if isinstance(report, dict) and report.get("error"):
            JOBS[job_id]["status"] = "failed"
            JOBS[job_id]["progress"] = 100
            JOBS[job_id]["message"] = report["error"]
            return

        JOBS[job_id]["progress"] = 90
        JOBS[job_id]["message"] = "Building refinement profile..."

        profile = build_refinement_profile(report)

        JOBS[job_id]["status"] = "completed"
        JOBS[job_id]["progress"] = 100
        JOBS[job_id]["message"] = "Completed"
        JOBS[job_id]["result"] = profile

    except Exception as e:
        JOBS[job_id]["status"] = "failed"
        JOBS[job_id]["progress"] = 100
        JOBS[job_id]["message"] = str(e)
        JOBS[job_id]["traceback"] = traceback.format_exc()


# -----------------------------
# Root (optional, avoids 404 on "/")
# -----------------------------
@app.get("/")
def root():
    return {"status": "ok", "docs": "/docs"}


# ---------------------------------------------------------------------- #
# 1) Compute similarities (like main page analysis)
# ---------------------------------------------------------------------- #
@app.get("/api/similarities")
def api_compute_similarities(
    folder: Optional[str] = Query(None, description="Folder path; omit for DataGEMS API mode"),
    kw: float = Query(0.6, ge=0, description="Weight for keywords similarity"),
    desc: float = Query(0.3, ge=0, description="Weight for description similarity"),
    head: float = Query(0.1, ge=0, description="Weight for headline similarity"),
    th: float = Query(30.0, ge=0, le=100, description="Threshold percentage"),
    include_chunks: bool = Query(False, description="Include expensive description chunk evidence"),
    include_graphs: bool = Query(False, description="Generate PGJSON graph for each pair"),
    graphs_only_above_threshold: bool = Query(True, description="Generate graphs only for pairs above threshold"),
    keyword_method: str = Query("jaccard", description="Keyword similarity method: jaccard or sbert"),
    force_refresh: bool = Query(False, description="Ignore all similarity caches and recompute from scratch"),
):
    kw, desc, head, normalized = normalize_weights(kw, desc, head)
    keyword_method = _keyword_method(keyword_method)

    folder = _normalize_folder_path(folder) if folder else None
    error, similarities, from_cache = compute_similarities(
        folder, kw, desc, head, threshold=th, use_api=folder is None,
        include_description_chunks=include_chunks,
        keyword_method=keyword_method,
        force_refresh=force_refresh,
    )
    if error:
        raise HTTPException(status_code=400, detail=error)

    graph_summary = None
    if include_graphs:
        graph_summary = _attach_graphs_to_similarities(
            folder,
            similarities,
            kw,
            desc,
            head,
            th,
            only_above_threshold=graphs_only_above_threshold,
        )

    return {
        "results": similarities,
        "from_cache": from_cache,
        "force_refresh": force_refresh,
        "threshold": th,
        "graphs": graph_summary,
        "weights": {
            "keywords": kw,
            "description": desc,
            "headline": head,
            "normalized": normalized,
            "include_chunks": include_chunks,
            "include_graphs": include_graphs,
            "graphs_only_above_threshold": graphs_only_above_threshold,
            "keyword_similarity_method": keyword_method,
        },
    }


# ---------------------------------------------------------------------- #
# 2) Single pair similarity (like selecting a row)
# ---------------------------------------------------------------------- #
@app.get("/api/similarity/single")
def api_single_similarity(
    folder: str = Query(...),
    d1: str = Query(..., description="First profile filename (e.g., a.json)"),
    d2: str = Query(..., description="Second profile filename (e.g., b.json)"),
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
    keyword_method: str = Query("jaccard", description="Keyword similarity method: jaccard or sbert"),
):
    kw, desc, head, normalized = normalize_weights(kw, desc, head)
    keyword_method = _keyword_method(keyword_method)

    error, similarities, from_cache = compute_similarities(
        folder, kw, desc, head, threshold=th, keyword_method=keyword_method
    )
    if error:
        raise HTTPException(status_code=400, detail=error)

    match = next(
        (
            s for s in similarities
            if (s["dataprofile1"] == d1 and s["dataprofile2"] == d2)
            or (s["dataprofile1"] == d2 and s["dataprofile2"] == d1)
        ),
        None,
    )
    if not match:
        raise HTTPException(status_code=404, detail=f"Pair {d1}/{d2} not found.")

    if not match.get("description_top_chunks"):
        match = dict(match)
        match["description_top_chunks"] = build_description_top_chunks_for_pair(
            folder,
            match.get("id1"),
            match.get("id2"),
            use_api=False
        )

    return {
        "match": match,
        "from_cache": from_cache,
        "threshold": th,
        "weights": {
            "keywords": kw,
            "description": desc,
            "headline": head,
            "normalized": normalized,
            "keyword_similarity_method": keyword_method,
        },
    }


# ---------------------------------------------------------------------- #
# 3) Build Croissant-like report JSON (like Save Results)
#    - JSON view
# ---------------------------------------------------------------------- #
@app.get("/api/report")
def api_build_report(
    folder: Optional[str] = Query(None),
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
    include_graphs: bool = Query(False, description="Generate PGJSON graph for each pair"),
    graphs_only_above_threshold: bool = Query(True, description="Generate graphs only for pairs above threshold"),
    keyword_method: str = Query("jaccard", description="Keyword similarity method: jaccard or sbert"),
    force_refresh: bool = Query(False, description="Ignore all similarity caches and recompute from scratch"),
):
    kw, desc, head, normalized = normalize_weights(kw, desc, head)
    keyword_method = _keyword_method(keyword_method)

    folder = _normalize_folder_path(folder) if folder else None
    error, similarities, from_cache = compute_similarities(
        folder, kw, desc, head, threshold=th, use_api=folder is None,
        keyword_method=keyword_method,
        force_refresh=force_refresh,
    )
    if error:
        raise HTTPException(status_code=400, detail=error)

    graph_summary = None
    if include_graphs:
        graph_summary = _attach_graphs_to_similarities(
            folder,
            similarities,
            kw,
            desc,
            head,
            th,
            only_above_threshold=graphs_only_above_threshold,
        )

    weights = {
        "keywords": kw,
        "description": desc,
        "headline": head,
        "normalized": normalized,
        "threshold": th,
        "include_graphs": include_graphs,
        "graphs_only_above_threshold": graphs_only_above_threshold,
        "keyword_similarity_method": keyword_method,
    }

    report = build_croissant_report(folder, weights, similarities)  # file_data optional
    report["from_cache"] = from_cache
    report["force_refresh"] = force_refresh
    report["graphs"] = graph_summary
    return report


# ----------------------------------------------------------------------
# 4) Compare DataLinkingBase links (Flask: /compare_links)
# ----------------------------------------------------------------------
class CompareLinksRequest(BaseModel):
    folder: Optional[str] = None
    kw: float = Field(0.6, ge=0)
    desc: float = Field(0.3, ge=0)
    head: float = Field(0.1, ge=0)
    th: float = Field(30.0, ge=0, le=100)
    relation_th: float = Field(0.0, ge=0, le=1)
    top_n: Optional[int] = Field(None, ge=1)
    selected_pairs: List[List[str]] = Field(default_factory=list)
    keyword_method: str = "jaccard"
    force_refresh: bool = False


@app.post("/api/compare-links")
def api_compare_links(req: CompareLinksRequest):
    folder = _normalize_folder_path(req.folder) if req.folder else None
    kw, desc, head, normalized = normalize_weights(req.kw, req.desc, req.head)
    keyword_method = _keyword_method(req.keyword_method)
    error, similarities, from_cache = compute_similarities(
        folder,
        kw,
        desc,
        head,
        threshold=req.th,
        use_api=folder is None,
        keyword_method=keyword_method,
        force_refresh=req.force_refresh,
    )
    if error:
        raise HTTPException(status_code=400, detail=error)

    selected_keys = {
        frozenset(str(value) for value in pair)
        for pair in req.selected_pairs
        if len(pair) == 2
    }
    links = [item for item in similarities if item.get("passes_threshold")]
    if selected_keys:
        links = [
            item for item in links
            if frozenset((str(item.get("id1")), str(item.get("id2")))) in selected_keys
        ]

    weights = {
        "keywords": kw,
        "description": desc,
        "headline": head,
        "normalized": normalized,
        "threshold": req.th,
        "keyword_similarity_method": keyword_method,
    }
    base_report = build_croissant_report(folder or "API_Datagems", weights, links, {})
    comparisons = compare_datalinkingbase_links(
        base_report["links"],
        relation_threshold=req.relation_th,
        top_n=req.top_n,
    )
    return {
        "@context": "http://mlcommons.org/croissant/",
        "@type": "DataLinkingBaseComparisonReport",
        "source": folder or "API_Datagems",
        "from_cache": from_cache,
        "force_refresh": req.force_refresh,
        "selection": {
            "mode": "selected_links" if selected_keys else "all_above_threshold",
            "minimum_combined_similarity": req.th,
            "minimum_relation_similarity": req.relation_th,
            "links_considered": len(base_report["links"]),
            "selected_pairs": len(selected_keys),
        },
        "weights": weights,
        "comparisons": comparisons,
    }


# ----------------------------------------------------------------------
# 5) Build composite DataLinking elements (Flask: /composite_links)
# ----------------------------------------------------------------------
@app.get("/api/composites")
@app.get("/api/build-composites", include_in_schema=False)
def api_build_composites(
    folder: Optional[str] = Query(None, description="Profile folder; omit for DataGEMS API mode"),
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
    relation_th: float = Query(0.5, ge=0, le=1),
    top_n: Optional[int] = Query(None, ge=1),
    keyword_method: str = Query("jaccard"),
    force_refresh: bool = Query(False, description="Ignore all caches and recompute from scratch"),
):
    folder = _normalize_folder_path(folder) if folder else None
    kw, desc, head, normalized = normalize_weights(kw, desc, head)
    keyword_method = _keyword_method(keyword_method)
    error, similarities, from_cache = compute_similarities(
        folder,
        kw,
        desc,
        head,
        threshold=th,
        use_api=folder is None,
        include_description_chunks=True,
        keyword_method=keyword_method,
        force_refresh=force_refresh,
    )
    if error:
        raise HTTPException(status_code=400, detail=error)

    weights = {
        "keywords": kw,
        "description": desc,
        "headline": head,
        "normalized": normalized,
        "threshold": th,
        "keyword_similarity_method": keyword_method,
    }
    source = folder or "API_Datagems"
    base_report = build_croissant_report(source, weights, similarities, {})
    profile_chunks, profile_labels = _load_profiles_for_composites(folder)
    caches = _composite_cache_for(folder)
    composite_data = build_composite_datalinking_elements(
        base_report["links"],
        relation_threshold=relation_th,
        top_n=top_n,
        base_threshold=th / 100.0,
        profile_chunks=profile_chunks,
        profile_labels=profile_labels,
        profile_chunk_cache=caches["profile"],
        original_chunk_cache=caches["original"],
        force_refresh=force_refresh,
    )
    return {
        "@context": "http://mlcommons.org/croissant/",
        "@type": "CompositeDataLinkingElementReport",
        "source": source,
        "from_cache": from_cache,
        "force_refresh": force_refresh,
        "selection": {
            "mode": "all_pairs_with_green_parent_merges",
            "minimum_combined_similarity": th,
            "minimum_relation_similarity": relation_th,
            "links_considered": len(base_report["links"]),
            "green_base_links": sum(
                1 for link in base_report["links"]
                if float((link.get("metrics") or {}).get("combined_similarity") or 0) / 100.0 >= th / 100.0
            ),
            "comparisons_used": len(composite_data["comparisons"]),
            "recursive_comparisons_used": len(composite_data.get("recursive_comparisons", [])),
            "final_composite_count": len(composite_data.get("final_composites", [])),
            "includes_description_chunks": True,
        },
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


# ---------------------------------------------------------------------- #
# 4) Download full report as a file (like Save Results button)
# ---------------------------------------------------------------------- #
@app.get("/api/report/download")
def api_download_report(
    folder: Optional[str] = Query(None),
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
    include_chunks: bool = Query(False, description="Include expensive description chunk evidence"),
    include_graphs: bool = Query(False, description="Generate PGJSON graph for each pair"),
    graphs_only_above_threshold: bool = Query(True, description="Generate graphs only for pairs above threshold"),
    keyword_method: str = Query("jaccard", description="Keyword similarity method: jaccard or sbert"),
    force_refresh: bool = Query(False, description="Ignore all similarity caches and recompute from scratch"),
):
    folder = _normalize_folder_path(folder) if folder else None
    kw, desc, head, normalized = normalize_weights(kw, desc, head)
    keyword_method = _keyword_method(keyword_method)

    error, similarities, _ = compute_similarities(
        folder, kw, desc, head, threshold=th, use_api=folder is None,
        include_description_chunks=include_chunks,
        keyword_method=keyword_method,
        force_refresh=force_refresh,
    )
    if error:
        raise HTTPException(status_code=400, detail=error)

    graph_summary = None
    if include_graphs:
        graph_summary = _attach_graphs_to_similarities(
            folder,
            similarities,
            kw,
            desc,
            head,
            th,
            only_above_threshold=graphs_only_above_threshold,
        )

    weights = {
        "keywords": kw,
        "description": desc,
        "headline": head,
        "normalized": normalized,
        "threshold": th,
        "include_description_chunks": include_chunks,
        "include_graphs": include_graphs,
        "graphs_only_above_threshold": graphs_only_above_threshold,
        "keyword_similarity_method": keyword_method,
    }

    report = build_croissant_report(folder, weights, similarities)
    report["graphs"] = graph_summary
    filename = f"similarity_{_timestamp()}.json"
    return _json_download(report, filename)


# ---------------------------------------------------------------------- #
# 5) Download single pair report as a file (like Save single)
# ---------------------------------------------------------------------- #
@app.get("/api/report/pair/download")
def api_download_pair(
    folder: str = Query(...),
    d1: str = Query(...),
    d2: str = Query(...),
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
    keyword_method: str = Query("jaccard", description="Keyword similarity method: jaccard or sbert"),
):
    kw, desc, head, normalized = normalize_weights(kw, desc, head)
    keyword_method = _keyword_method(keyword_method)

    error, similarities, _ = compute_similarities(folder, kw, desc, head, threshold=th, keyword_method=keyword_method)
    if error:
        raise HTTPException(status_code=400, detail=error)

    match = next(
        (
            s for s in similarities
            if (s["dataprofile1"] == d1 and s["dataprofile2"] == d2)
            or (s["dataprofile1"] == d2 and s["dataprofile2"] == d1)
        ),
        None,
    )
    if not match:
        raise HTTPException(status_code=404, detail=f"Pair {d1}/{d2} not found.")

    if not match.get("description_top_chunks"):
        match = dict(match)
        match["description_top_chunks"] = build_description_top_chunks_for_pair(
            folder,
            match.get("id1"),
            match.get("id2"),
            use_api=False
        )

    # Minimal Croissant-like pair report (same structure as your Flask /save_single)
    output = {
        "@context": "http://mlcommons.org/croissant/",
        "@type": "DatasetSimilarityReport",
        "analyzedFolder": str(folder),
        "elements": [],
        "links": [],
        "weights": {
            "keywords": kw,
            "description": desc,
            "headline": head,
            "normalized": normalized,
            "threshold": th,
            "keyword_similarity_method": keyword_method,
        },
    }

    for dp in [match["dataprofile1"], match["dataprofile2"]]:
        output["elements"].append(
            {
                "@type": "DLElement",
                "@id": f"profile:{dp.replace('.json', '')}",
                "name": dp,
                "description": "",
                "keywords": [],
                "headline": "",
                "source": {
                    "@type": "DataDownload",
                    "contentUrl": f"file:///{folder}/{dp}",
                    "encodingFormat": "application/json",
                },
            }
        )

    output["links"].append(
        {
            "@type": "SimilarityLink",
            "@id": f"link:{_timestamp()}",
            "dataprofile1": f"profile:{match['dataprofile1'].replace('.json', '')}",
            "dataprofile2": f"profile:{match['dataprofile2'].replace('.json', '')}",
            "dataprofile1id": match.get("id1"),
            "dataprofile2id": match.get("id2"),
            "metrics": {
                "keywords_similarity": match["keywords_similarity"],
                "keyword_similarity_method": match.get("keyword_similarity_method", keyword_method),
                "description_similarity": match["description_similarity"],
                "headline_similarity": match["headline_similarity"],
                "combined_similarity": match["combined_similarity"],
                "headline_used_in_score": match.get("headline_used_in_score", True),
                "field_usage": match.get("field_usage"),
                "effective_weights": match.get("effective_weights"),
            },
            "common_keywords": [
                k.strip()
                for k in (match.get("common_keywords") or "").split(",")
                if k.strip()
            ],
        }
    )

    filename = (
        f"similarity_{d1.replace('.json','')}__{d2.replace('.json','')}_{_timestamp()}.json"
    )
    return _json_download(output, filename)


# ---------------------------------------------------------------------- #
# 6) Refine (where possible) - JSON response
# ---------------------------------------------------------------------- #
@app.get("/api/refine")
def api_refine(
    folder: str = Query(...),
    d1: str = Query(...),
    d2: str = Query(...),
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
):
    # Keep signature compatible: refine_similarity currently ignores weights/threshold,
    # but we pass them anyway for consistency.
    report = refine_similarity(folder, d1, d2, kw, desc, head, th)
    if isinstance(report, dict) and report.get("error"):
        raise HTTPException(status_code=400, detail=report["error"])
    return report


# ---------------------------------------------------------------------- #
# 7) Refine download - Croissant-like refinement profile file
# ---------------------------------------------------------------------- #
@app.get("/api/refine/download")
def api_refine_download(
    folder: str = Query(...),
    d1: str = Query(...),
    d2: str = Query(...),
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
):
    report = refine_similarity(folder, d1, d2, kw, desc, head, th)
    if isinstance(report, dict) and report.get("error"):
        raise HTTPException(status_code=400, detail=report["error"])

    profile = build_refinement_profile(report)
    filename = f"{d1.replace('.json','')}__{d2.replace('.json','')}.refinement.json"
    return _json_download(profile, filename)


# ---------------------------------------------------------------------- #
# 8) Similarities for selected profiles only (like filtering)
# ---------------------------------------------------------------------- #
class SelectProfilesRequest(BaseModel):
    folder: str
    profiles: List[str]
    kw: Optional[float] = 0.6
    desc: Optional[float] = 0.3
    head: Optional[float] = 0.1
    th: Optional[float] = Field(30.0, ge=0, le=100)
    keyword_method: Optional[str] = "jaccard"
    force_refresh: bool = False


@app.post("/api/similarities/select")
def api_select_similarities(req: SelectProfilesRequest):
    if not req.profiles:
        raise HTTPException(status_code=400, detail="No profiles provided.")

    kw, desc, head, normalized = normalize_weights(req.kw, req.desc, req.head)
    th = float(req.th or 30.0)
    keyword_method = _keyword_method(req.keyword_method or "jaccard")

    error, all_similarities, from_cache = compute_similarities(
        req.folder, kw, desc, head, threshold=th, keyword_method=keyword_method,
        force_refresh=req.force_refresh,
    )
    if error:
        raise HTTPException(status_code=400, detail=error)

    selected = set(req.profiles)
    filtered = [
        s for s in all_similarities
        if s["dataprofile1"] in selected and s["dataprofile2"] in selected
    ]

    return {
        "results": filtered,
        "selected_profiles": req.profiles,
        "from_cache": from_cache,
        "force_refresh": req.force_refresh,
        "threshold": th,
        "weights": {
            "keywords": kw,
            "description": desc,
            "headline": head,
            "normalized": normalized,
            "keyword_similarity_method": keyword_method,
        },
    }

# ---------------------------------------------------------------------- #
# 9) Jobs - start report
# ---------------------------------------------------------------------- #
@app.post("/api/jobs/report")
def api_job_start_report(
        background_tasks: BackgroundTasks,
        # Rendi folder facoltativo con default None
        folder: Optional[str] = Query(None, description="Path or leave empty for API mode"),
        kw: float = Query(0.6, ge=0),
        desc: float = Query(0.3, ge=0),
        head: float = Query(0.1, ge=0),
        th: float = Query(30.0, ge=0, le=100),
        include_chunks: bool = Query(False, description="Include expensive description chunk evidence"),
        include_graphs: bool = Query(False, description="Generate PGJSON graph for each pair"),
        graphs_only_above_threshold: bool = Query(True, description="Generate graphs only for pairs above threshold"),
        keyword_method: str = Query("jaccard", description="Keyword similarity method: jaccard or sbert"),
        force_refresh: bool = Query(False, description="Ignore all caches and recompute from scratch"),
):
    if folder and (folder.strip() == "" or folder.strip().lower() == "none"):
        folder = None

    kw, desc, head, normalized = normalize_weights(kw, desc, head)
    keyword_method = _keyword_method(keyword_method)

    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        "job_id": job_id,
        "type": "report",
        "status": "queued",
        "progress": 0,
        "message": "Queued",
        "params": {
            "folder": folder,
            "kw": kw,
            "desc": desc,
            "head": head,
            "th": th,
            "normalized": normalized,
            "include_chunks": include_chunks,
            "include_graphs": include_graphs,
            "graphs_only_above_threshold": graphs_only_above_threshold,
            "keyword_similarity_method": keyword_method,
            "force_refresh": force_refresh,
        },
        "result": None,
    }
    background_tasks.add_task(
        _run_report_job,
        job_id,
        folder,
        kw,
        desc,
        head,
        th,
        include_chunks,
        include_graphs,
        graphs_only_above_threshold,
        keyword_method,
        force_refresh,
    )
    return {"job_id": job_id, "status": "queued"}


# ---------------------------------------------------------------------- #
# 10) Jobs - start refine
# ---------------------------------------------------------------------- #
@app.post("/api/jobs/refine")
def api_job_start_refine(
        background_tasks: BackgroundTasks,
        folder: Optional[str] = Query(None),
        d1: str = Query(...),
        d2: str = Query(...),
        kw: float = Query(0.6, ge=0),
        desc: float = Query(0.3, ge=0),
        head: float = Query(0.1, ge=0),
        th: float = Query(30.0, ge=0, le=100),
):
    if folder and (folder.strip() == "" or folder.strip().lower() == "none"):
        folder = None

    kw, desc, head, normalized = normalize_weights(kw, desc, head)

    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        "job_id": job_id,
        "type": "refine",
        "status": "queued",
        "progress": 0,
        "message": "Queued",
        "params": {
            "folder": folder,
            "d1": d1,
            "d2": d2,
            "kw": kw,
            "desc": desc,
            "head": head,
            "th": th,
            "normalized": normalized,
        },
        "result": None,
    }

    background_tasks.add_task(_run_refine_job, job_id, folder, d1, d2, kw, desc, head, th)
    return {"job_id": job_id, "status": "queued"}


# ---------------------------------------------------------------------- #
# 11) Jobs - status (polling)
# ---------------------------------------------------------------------- #
@app.get("/api/jobs/{job_id}")
def api_job_status(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job_id,
        "type": job.get("type"),
        "status": job.get("status"),
        "progress": job.get("progress", 0),
        "message": job.get("message", ""),
        "params": job.get("params", {}),
    }


# ---------------------------------------------------------------------- #
# 12) Jobs - result JSON
# ---------------------------------------------------------------------- #
@app.get("/api/jobs/{job_id}/result")
def api_job_result(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("status") != "completed":
        return {
            "job_id": job_id,
            "status": job.get("status"),
            "progress": job.get("progress", 0),
            "message": job.get("message", ""),
        }

    return job["result"]


# ---------------------------------------------------------------------- #
# 13) Jobs - result download
# ---------------------------------------------------------------------- #
@app.get("/api/jobs/{job_id}/download")
def api_job_download(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("status") != "completed":
        raise HTTPException(status_code=409, detail="Job not completed yet")

    payload = job["result"]
    job_type = job.get("type", "job")
    filename = f"{job_type}_{job_id}_{_timestamp()}.json"
    return _json_download(payload, filename)

@app.get("/api/discover-and-compare")
async def api_discover_and_compare(
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
    keyword_method: str = Query("jaccard", description="Keyword similarity method: jaccard or sbert"),
):
    """
    Download 'ready' profile by API
    """
    try:
        kw, desc, head, normalized = normalize_weights(kw, desc, head)
        keyword_method = _keyword_method(keyword_method)
        error, similarities, from_cache = compute_similarities(
            folder_path=None,
            kw_weight=kw,
            desc_weight=desc,
            head_weight=head,
            threshold=th,
            use_api=True,
            keyword_method=keyword_method
        )

        if error:
            raise HTTPException(status_code=400, detail=error)

        return {
            "status": "success",
            "results": similarities,
            "metadata": {
                "total_pairs": len(similarities),
                "from_cache": from_cache,
                "weights": {"kw": kw, "desc": desc, "head": head, "keyword_similarity_method": keyword_method},
                "threshold": th
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
