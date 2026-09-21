# Dataset Linking API Integration Guide

This document describes how another project module can call the Dataset Linking API, run an asynchronous analysis, retrieve its result, and pass the generated PGJSON graph to the next module.

## 1. Service

Run the FastAPI service from the repository root:

```bash
uv run uvicorn dl.fastapi_app:app --host 0.0.0.0 --port 8000
```

The local base URL is:

```text
http://127.0.0.1:8000
```

Interactive OpenAPI documentation is available at:

```text
http://127.0.0.1:8000/docs
```

When the service is deployed behind a reverse proxy, use the deployment URL and any configured `/dl` prefix.

## 2. Integration Boundary

The previous module must provide either:

- a local folder containing dataset profile JSON files; or
- no folder, in which case the service retrieves ready profiles from the configured DataGEMS API.

The local profile format can be a raw Croissant-like object:

```json
{
  "@id": "dataset-a",
  "name": "Dataset A",
  "headline": "A short dataset headline",
  "description": "A dataset description with enough text for semantic comparison.",
  "keywords": ["keyword-a", "shared-topic"],
  "distribution": [],
  "recordSet": []
}
```

Profiles may also be wrapped as follows:

```json
{
  "dataset": {
    "@id": "dataset-a",
    "name": "Dataset A"
  }
}
```

For local mode, the folder is passed through the `folder` query parameter. Every JSON file in the top level of the folder is treated as a profile except files whose names start with `cache_`.

## 3. Similarity Parameters

The analysis accepts these common parameters:

| Parameter | Default | Meaning |
|---|---:|---|
| `kw` | `0.6` | Keyword contribution |
| `desc` | `0.3` | Description contribution |
| `head` | `0.1` | Headline contribution |
| `th` | `30.0` | Pair similarity threshold, expressed as a percentage |
| `keyword_method` | `jaccard` | `jaccard` or `sbert` where supported |
| `include_chunks` | `false` | Include pair description chunk evidence |
| `include_graphs` | `false` | Generate a PGJSON graph for eligible pairs |
| `graphs_only_above_threshold` | `true` | Generate graphs only for pairs passing `th` |

Weights are normalized if their sum is not `1.0`. Effective weights can differ for a pair when a profile does not contain usable keywords, descriptions, or headlines.

## 4. Recommended Job Workflow

The recommended integration uses asynchronous jobs:

1. Start a job.
2. Store the returned `job_id`.
3. Poll the status endpoint.
4. Wait for `status` to become `completed`.
5. Retrieve the result or download it.
6. Extract the PGJSON graph if the next module needs the graph only.

Jobs are stored in the service process memory. A service restart removes queued, running, and completed jobs.

## 5. Report Job

Use the report job to compare all available dataset pairs.

### Start a report job

API mode, where profiles are fetched from DataGEMS:

```bash
curl -X POST \
  "http://127.0.0.1:8000/api/jobs/report?kw=0.6&desc=0.3&head=0.1&th=30&include_chunks=true&include_graphs=true&graphs_only_above_threshold=true&keyword_method=jaccard"
```

Local-folder mode:

```bash
curl -X POST \
  --get "http://127.0.0.1:8000/api/jobs/report" \
  --data-urlencode "folder=demo_profiles" \
  --data-urlencode "kw=0.6" \
  --data-urlencode "desc=0.3" \
  --data-urlencode "head=0.1" \
  --data-urlencode "th=30" \
  --data-urlencode "include_chunks=true" \
  --data-urlencode "include_graphs=true" \
  --data-urlencode "graphs_only_above_threshold=true" \
  --data-urlencode "keyword_method=jaccard"
```

The response only acknowledges the job:

```json
{
  "job_id": "9fb2...",
  "status": "queued"
}
```

### Poll the job

```bash
curl "http://127.0.0.1:8000/api/jobs/9fb2..."
```

Possible states are:

```text
queued
in_progress
completed
failed
```

Example while running:

```json
{
  "job_id": "9fb2...",
  "type": "report",
  "status": "in_progress",
  "progress": 70,
  "message": "Generating pair graphs...",
  "params": {
    "include_graphs": true
  }
}
```

### Retrieve the report result

```bash
curl "http://127.0.0.1:8000/api/jobs/9fb2.../result"
```

The completed report has a wrapper similar to:

```json
{
  "@type": "DatasetSimilarityReport",
  "links": [
    {
      "@type": "DataLinkingBase",
      "dataprofile1ref": "dataset-a",
      "dataprofile2ref": "dataset-b",
      "metrics": {
        "combined_similarity": 72.4
      },
      "graph": {
        "nodes": [],
        "edges": []
      }
    }
  ],
  "graphs": {
    "generated": 1,
    "failed": 0,
    "skipped": 0
  }
}
```

The graph is present under `links[i].graph` only when `include_graphs=true` and the pair is eligible according to `graphs_only_above_threshold`.

### Download the report

```bash
curl -OJ "http://127.0.0.1:8000/api/jobs/9fb2.../download"
```

The downloaded file contains the same completed job result as `/result`.

## 6. Refine Job

Use the refine job for one selected pair. This job requires a local folder and two profile filenames or identifiers resolvable in that folder.

### Start a refine job

```bash
curl -X POST \
  --get "http://127.0.0.1:8000/api/jobs/refine" \
  --data-urlencode "folder=demo_profiles" \
  --data-urlencode "d1=weather_observations.json" \
  --data-urlencode "d2=climate_indicators.json" \
  --data-urlencode "kw=0.6" \
  --data-urlencode "desc=0.3" \
  --data-urlencode "head=0.1" \
  --data-urlencode "th=30"
```

The response is:

```json
{
  "job_id": "f31a...",
  "status": "queued"
}
```

Poll and retrieve the result with the same job endpoints:

```bash
curl "http://127.0.0.1:8000/api/jobs/f31a..."
curl "http://127.0.0.1:8000/api/jobs/f31a.../result"
curl -OJ "http://127.0.0.1:8000/api/jobs/f31a.../download"
```

A completed refine result is a refinement wrapper:

```json
{
  "@type": "RefinementReport",
  "generatedAtTime": "2026-09-21T10:00:00Z",
  "graph": {
    "nodes": [],
    "edges": []
  },
  "results": {
    "matching_file_table_names": [],
    "matching_keywords": [],
    "matching_csv_table_column_names": [],
    "data_overlaps": []
  }
}
```

For this job, the PGJSON graph is directly under `result.graph`.

## 7. PGJSON Contract

The pure graph passed to a downstream graph-registration module is:

```json
{
  "nodes": [
    {
      "id": "root-id",
      "labels": ["BasicDLElement"],
      "properties": {
        "similarityScore": 0.72,
        "usesMetrics": "avg"
      }
    },
    {
      "id": "dataset-a",
      "labels": ["sc:Dataset"],
      "properties": {}
    },
    {
      "id": "dataset-b",
      "labels": ["sc:Dataset"],
      "properties": {}
    }
  ],
  "edges": [
    {
      "from": "root-id",
      "to": "dataset-a",
      "labels": ["HAS_TARGET"]
    },
    {
      "from": "root-id",
      "to": "dataset-b",
      "labels": ["HAS_TARGET"]
    }
  ]
}
```

The root `BasicDLElement` must directly reference exactly two existing `sc:Dataset` nodes through `HAS_TARGET` edges. A pair must not be registered more than once.

Do not upload the complete job wrapper when the downstream endpoint expects PGJSON. Extract one of these fields instead:

```text
Report job:  result.links[i].graph
Refine job:  result.graph
```

Example extraction with Python:

```python
import json

with open("report.json", encoding="utf-8") as source:
    report = json.load(source)

graph = report["links"][0]["graph"]       # report job
# graph = report["graph"]                  # refine job

with open("dataset-linking.json", "w", encoding="utf-8") as target:
    json.dump(graph, target, indent=2, ensure_ascii=False)
```

## 8. Errors and Operational Notes

| Status | Meaning |
|---:|---|
| `200` | Request or result completed successfully |
| `400` | Invalid analysis parameters or upstream API error |
| `404` | Unknown job or resource |
| `409` | Result requested before the job completed |
| `422` | Request validation failed |

The service logs progress to the terminal. A failed job exposes an error message through the status endpoint and may include a traceback in the in-memory job record.

Results may include `from_cache: true`. This means the similarity data was loaded from the local/API cache; it does not change the result contract.

For large analyses, use the job endpoints rather than keeping a synchronous HTTP request open. The caller should poll every few seconds and stop polling after `completed` or `failed`.

## 9. Relevant Source Files

```text
dl/fastapi_app.py       API routes and asynchronous job lifecycle
dl/similarity.py        Profile loading, pair scores and caching
dl/refine.py            Refinement logic and PGJSON generation
dl/reports.py           Similarity report wrapper
dl/link_comparison.py   Composite and link comparison logic
```
