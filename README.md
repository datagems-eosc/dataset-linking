# Dataset Linking

Web application and REST API for computing semantic links between Croissant-like dataset profiles.

The project provides:

- A Flask UI for interactive analysis, comparison, refinement, and JSON downloads.
- A FastAPI backend for programmatic analysis, report generation, refinement, asynchronous jobs, and PGJSON graph output.
- A similarity engine based on keywords, descriptions, and headlines with configurable weights.
- Refinement logic that inspects file objects, tabular structures, text evidence, and shared resources.

## Requirements

- Python 3.12 recommended
- `uv` recommended for dependency management

Install dependencies from the existing lock files:

```bash
uv venv
uv sync
```

## Run The Flask App

From the project root:

```bash
uv run flask --app dl.flask_app run
```

The UI is available at:

```text
http://127.0.0.1:5000
```

The Flask UI supports:

- Selecting a local folder containing JSON profiles.
- Running pairwise similarity analysis.
- Saving the full report or a single pair report.
- Refining a selected pair and generating PGJSON graph output on the Desktop as `grafo_visualizer.json`.
- Comparing generated `DataLinkingBase` links through `Compare Links` and `Compare Selected`.

## Run The FastAPI App

From the project root:

```bash
python -m uvicorn dl.fastapi_app:app --host 0.0.0.0 --port 8000 --reload
```

Open Swagger UI at:

```text
http://127.0.0.1:8000/docs
```

## Input Profiles

Local analysis expects a folder containing JSON dataset profiles:

```text
Profiles/
├── dataset1.json
├── dataset2.json
└── dataset3.json
```

Profiles may be raw Croissant dataset objects or wrapped as:

```json
{
  "dataset": { ... }
}
```

API mode can fetch profiles from the DataGEMS platform through the configured platform endpoints.

## Similarity Scoring

The base similarity analysis computes one score per dataset pair using:

- `keywords_similarity`
- `description_similarity`
- `headline_similarity`
- `combined_similarity`

Default requested weights are:

```text
keywords:    0.6
description: 0.3
headline:    0.1
```

If the requested weights do not sum to `1.0`, they are normalized.

Effective weights may vary per pair:

- Keywords contribute when at least one of the two profiles has keywords.
- Description contributes only when both profiles have a usable description.
- Headline contributes only when both profiles have usable headline text.
- Available weights are renormalized to 100% for that pair.

The output includes:

```json
"field_usage": {
  "keywords": true,
  "description": true,
  "headline": false
},
"effective_weights": {
  "keywords": 0.6667,
  "description": 0.3333,
  "headline": 0.0
}
```

When a text field is excluded, the Flask table displays `excluded` instead of showing misleading similarity values.

## Description Chunk Evidence

Description chunk extraction is optional because it can be expensive on large runs.

- Standard analysis does not compute massive chunk evidence by default.
- Single-pair downloads/refinement can compute pair-specific description chunks on demand.
- FastAPI endpoints expose `include_chunks` where applicable.

## FastAPI Endpoints

Main endpoints:

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/` | Health/root response |
| `GET` | `/api/similarities` | Compute all pairwise similarities |
| `GET` | `/api/similarity/single` | Return one pair similarity with optional pair chunk evidence |
| `GET` | `/api/report` | Build a report JSON |
| `GET` | `/api/report/download` | Download a report JSON |
| `GET` | `/api/report/pair/download` | Download a single pair report |
| `GET` | `/api/refine` | Run refinement for one pair and return graph data |
| `GET` | `/api/refine/download` | Download one refinement report |
| `POST` | `/api/similarities/select` | Compute similarities for selected profiles |
| `POST` | `/api/jobs/report` | Start an async report job |
| `POST` | `/api/jobs/refine` | Start an async refine job |
| `GET` | `/api/jobs/{job_id}` | Poll job status |
| `GET` | `/api/jobs/{job_id}/result` | Get completed job result |
| `GET` | `/api/jobs/{job_id}/download` | Download completed job result |
| `GET` | `/api/discover-and-compare` | Fetch platform profiles and compare them |

## PGJSON Graph Output

Refinement generates PGJSON-style graph data with:

```json
{
  "nodes": [...],
  "edges": [...]
}
```

The graph is generated centrally in:

```text
dl/refine.py -> build_graph_json(...)
```

It is used by both Flask and FastAPI.

Core graph node labels:

- `BasicDLElement`
- `PropertyComparison`
- `TextEvidence`
- `sc:Dataset`

When common file/table objects exist, the graph also includes FO detail nodes:

- `FileObjectLinkingElement`
- `FileObjectComparison`
- `cr:FileObject`

Core edge labels:

- `HAS_TARGET`
- `HAS_COMPARISON`
- `HAS_EVIDENCE`
- `HAS_DETAIL`
- `distribution`

The FO block is created only when file/table names match after normalization. It no longer creates file-object evidence from generic sample overlaps, because those can be too noisy.

## Graphs During FastAPI Analysis

FastAPI can attach PGJSON graphs directly to similarity analysis results.

Supported parameters on relevant report/similarity endpoints:

```text
include_graphs=true
graphs_only_above_threshold=true
```

For example:

```text
GET /api/similarities?folder=...&include_graphs=true&graphs_only_above_threshold=true
```

Each generated pair can contain:

```json
"graph": {
  "nodes": [...],
  "edges": [...]
}
```

The response also includes a graph generation summary:

```json
"graphs": {
  "generated": 10,
  "failed": 0,
  "skipped": 35
}
```

For large folders, prefer the async job endpoint:

```text
POST /api/jobs/report?include_graphs=true
```

Then poll/download via the job endpoints.

## Link Comparison

The Flask UI can compare generated `DataLinkingBase` links.

The comparison page reports:

- relation similarity
- shared profiles
- shared profile files/columns
- shared keywords
- keyword overlap
- metric similarity
- effective link weights
- refinement overlap for files, columns, and samples when available

`DataLinkingBaseComparison` logic is implemented in:

```text
dl/link_comparison.py
```

## Caching

Result and embedding caches are used to avoid repeated expensive computations.

API mode uses the persistent cache location:

```text
/s3/cache
```

Local mode stores cache data close to the selected local folder.

Cache fingerprints include source signatures, weights, local file content fingerprints, and scoring-mode versions where relevant.

## Utility Scripts

Useful scripts under `dl/`:

- `token.py`: downloads platform Croissant profiles into a local `profiles_api` folder.
- `count_datasets.py`: compares platform search counts with Croissant detail statuses.
- `export_excel.py`: exports similarity results to Excel.
- `run_refinement.py`: helper for refinement experiments.

## Project Structure

```text
dl/
├── fastapi_app.py        FastAPI REST API and async jobs
├── flask_app.py          Flask UI routes
├── similarity.py         Similarity engine, API/local profile loading, caching
├── refine.py             Refinement logic and PGJSON graph generation
├── reports.py            DataLinkingBase report builder
├── link_comparison.py    DataLinkingBaseComparison logic
├── utils.py              Shared helpers
├── token.py              Platform profile downloader
├── count_datasets.py     Platform status/count diagnostic script
└── export_excel.py       Excel export helper

templates/
├── index.html
└── link_comparisons.html

static/
└── style.css
```

## Models

- `all-MiniLM-L6-v2`: short text embeddings, mainly headline scoring.
- `all-mpnet-base-v2`: long text embeddings, mainly description scoring and chunk evidence.

Models are loaded lazily when first needed.

## Notes

- The temporary branch `temp/sbert-keywords-local-analysis` contains an experimental local-analysis variant where keyword similarity is SBERT-based. It is not part of the stable `main` behavior described here.
- Generated files such as `__pycache__`, local Excel reports, and downloaded `profiles_api` JSON files should not be committed unless explicitly needed.

## Author

Developed by Stefano Tanfoglio for the Master's Thesis Project - Data Linking, University of Verona, 2026.
