# dl/fastapi_app.py
import io
import json
import uuid
import traceback
from datetime import datetime
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from dl.refine import refine_similarity, build_refinement_profile
from dl.reports import build_croissant_report
from dl.similarity import compute_similarities
from dl.utils import normalize_weights

app = FastAPI(title="Profile Similarity API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _json_download(payload: dict, filename: str) -> StreamingResponse:
    data = json.dumps(payload, ensure_ascii=False, indent=4).encode("utf-8")
    buf = io.BytesIO(data)
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(buf, media_type="application/json", headers=headers)

JOBS: Dict[str, Dict[str, Any]] = {}


def _normalize_folder_path(folder: str) -> str:
    return (folder or "").strip().replace("\\", "/")


def _run_report_job(job_id: str, folder: str, kw: float, desc: float, head: float, th: float) -> None:
    try:
        JOBS[job_id]["status"] = "in_progress"
        JOBS[job_id]["progress"] = 5
        JOBS[job_id]["message"] = "Starting report job..."

        folder = _normalize_folder_path(folder)

        JOBS[job_id]["progress"] = 20
        JOBS[job_id]["message"] = "Computing similarities..."

        error, similarities, from_cache = compute_similarities(folder, kw, desc, head, threshold=th)
        if error:
            JOBS[job_id]["status"] = "failed"
            JOBS[job_id]["progress"] = 100
            JOBS[job_id]["message"] = error
            return

        JOBS[job_id]["progress"] = 85
        JOBS[job_id]["message"] = "Building report..."

        weights = {
            "keywords": kw,
            "description": desc,
            "headline": head,
            "normalized": JOBS[job_id]["params"]["normalized"],
            "threshold": th,
        }

        report = build_croissant_report(folder, weights, similarities)
        report["from_cache"] = from_cache

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
    folder: str = Query(..., description="Folder path containing JSON profiles"),
    kw: float = Query(0.6, ge=0, description="Weight for keywords similarity"),
    desc: float = Query(0.3, ge=0, description="Weight for description similarity"),
    head: float = Query(0.1, ge=0, description="Weight for headline similarity"),
    th: float = Query(30.0, ge=0, le=100, description="Threshold percentage"),
):
    kw, desc, head, normalized = normalize_weights(kw, desc, head)

    error, similarities, from_cache = compute_similarities(
        folder, kw, desc, head, threshold=th
    )
    if error:
        raise HTTPException(status_code=400, detail=error)

    return {
        "results": similarities,
        "from_cache": from_cache,
        "threshold": th,
        "weights": {
            "keywords": kw,
            "description": desc,
            "headline": head,
            "normalized": normalized,
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
):
    kw, desc, head, normalized = normalize_weights(kw, desc, head)

    error, similarities, from_cache = compute_similarities(
        folder, kw, desc, head, threshold=th
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

    return {
        "match": match,
        "from_cache": from_cache,
        "threshold": th,
        "weights": {
            "keywords": kw,
            "description": desc,
            "headline": head,
            "normalized": normalized,
        },
    }


# ---------------------------------------------------------------------- #
# 3) Build Croissant-like report JSON (like Save Results)
#    - JSON view
# ---------------------------------------------------------------------- #
@app.get("/api/report")
def api_build_report(
    folder: str = Query(...),
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
):
    kw, desc, head, normalized = normalize_weights(kw, desc, head)

    error, similarities, from_cache = compute_similarities(
        folder, kw, desc, head, threshold=th
    )
    if error:
        raise HTTPException(status_code=400, detail=error)

    weights = {
        "keywords": kw,
        "description": desc,
        "headline": head,
        "normalized": normalized,
        "threshold": th,
    }

    report = build_croissant_report(folder, weights, similarities)  # file_data optional
    report["from_cache"] = from_cache
    return report


# ---------------------------------------------------------------------- #
# 4) Download full report as a file (like Save Results button)
# ---------------------------------------------------------------------- #
@app.get("/api/report/download")
def api_download_report(
    folder: str = Query(...),
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
):
    kw, desc, head, normalized = normalize_weights(kw, desc, head)

    error, similarities, _ = compute_similarities(folder, kw, desc, head, threshold=th)
    if error:
        raise HTTPException(status_code=400, detail=error)

    weights = {
        "keywords": kw,
        "description": desc,
        "headline": head,
        "normalized": normalized,
        "threshold": th,
    }

    report = build_croissant_report(folder, weights, similarities)
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
):
    kw, desc, head, normalized = normalize_weights(kw, desc, head)

    error, similarities, _ = compute_similarities(folder, kw, desc, head, threshold=th)
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
                "description_similarity": match["description_similarity"],
                "headline_similarity": match["headline_similarity"],
                "combined_similarity": match["combined_similarity"],
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


@app.post("/api/similarities/select")
def api_select_similarities(req: SelectProfilesRequest):
    if not req.profiles:
        raise HTTPException(status_code=400, detail="No profiles provided.")

    kw, desc, head, normalized = normalize_weights(req.kw, req.desc, req.head)
    th = float(req.th or 30.0)

    error, all_similarities, from_cache = compute_similarities(
        req.folder, kw, desc, head, threshold=th
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
        "threshold": th,
        "weights": {
            "keywords": kw,
            "description": desc,
            "headline": head,
            "normalized": normalized,
        },
    }

# ---------------------------------------------------------------------- #
# 9) Jobs - start report
# ---------------------------------------------------------------------- #
@app.post("/api/jobs/report")
def api_job_start_report(
    background_tasks: BackgroundTasks,
    folder: str = Query(...),
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
):
    kw, desc, head, normalized = normalize_weights(kw, desc, head)

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
        },
        "result": None,
    }

    background_tasks.add_task(_run_report_job, job_id, folder, kw, desc, head, th)
    return {"job_id": job_id, "status": "queued"}


# ---------------------------------------------------------------------- #
# 10) Jobs - start refine
# ---------------------------------------------------------------------- #
@app.post("/api/jobs/refine")
def api_job_start_refine(
    background_tasks: BackgroundTasks,
    folder: str = Query(...),
    d1: str = Query(...),
    d2: str = Query(...),
    kw: float = Query(0.6, ge=0),
    desc: float = Query(0.3, ge=0),
    head: float = Query(0.1, ge=0),
    th: float = Query(30.0, ge=0, le=100),
):
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
