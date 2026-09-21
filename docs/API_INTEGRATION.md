# Dataset Linking API — Upstream Integration Guide

This document defines the integration contract between an **upstream project module** and the **Dataset Linking** service.

The main goal is to explain what the previous module must provide and how it can start Dataset Linking processing through the asynchronous `report` and `refine` jobs.

The downstream graph-registration/output contract is intentionally out of scope here, except for a short note at the end.

---

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

The application is configured with FastAPI `root_path="/dl"`. When it is deployed behind a reverse proxy, use the deployment URL and the configured `/dl` prefix where applicable.

Examples in this document use the direct local URL without the prefix.

## 2. Upstream Integration Boundary

The upstream module interacts with Dataset Linking in one of two ways:

| Operation | Purpose | Input source |
|---|---|---|
| `report` | Compare all available dataset profiles and produce pairwise similarity analysis | Local shared folder **or** configured DataGEMS API |
| `refine` | Run detailed refinement for one selected pair | Two DataGEMS dataset IDs by default, or two local profile files from a shared folder |

The two operations are separate entry points. A `refine` job does not need the `job_id` of a previous `report` job.

### 2.1 What the upstream module is responsible for

For the normal platform integration, the upstream module should:

1. Identify the datasets already available in DataGEMS.
2. For `refine`, pass the two DataGEMS dataset IDs as `d1` and `d2`; Dataset Linking retrieves the corresponding profiles itself.
3. For `report`, omit `folder` to let Dataset Linking retrieve the available ready profiles from the configured DataGEMS API.
4. Store the returned `job_id` and optionally poll the job status until it reaches a terminal state.

Local-folder mode remains available when profiles are exchanged through shared storage. In that case, the upstream module must make the files visible to the Dataset Linking runtime and pass the shared path as `folder`. For local `refine`, `d1` and `d2` identify the two profile files in that folder.

The upstream module does **not** need to understand or forward the final PGJSON graph in order to start Dataset Linking processing.

## 3. Filesystem and Container Contract

### 3.1 `folder` is resolved by Dataset Linking

The `folder` query parameter is a filesystem path interpreted by the **Dataset Linking process/container**, not by the caller.

For example, this request:

```text
folder=/exchange/profiles
```

means that `/exchange/profiles` must exist and be readable **inside the Dataset Linking runtime**.

If the upstream module and Dataset Linking run in different containers or services, a path that exists only inside the upstream container is not sufficient.

### 3.2 Recommended shared-volume setup

A common deployment is to mount the same storage into both services:

```yaml
services:
  upstream-module:
    volumes:
      - shared-profiles:/exchange

  dataset-linking:
    volumes:
      - shared-profiles:/exchange

volumes:
  shared-profiles:
```

The upstream module writes profiles under `/exchange/profiles`, and then calls Dataset Linking with:

```text
folder=/exchange/profiles
```

The exact storage technology can be different; the requirement is simply that Dataset Linking can read the path it receives.

### 3.3 Folder contents

Use a directory dedicated to Dataset Linking profile files. Avoid mixing unrelated JSON files into the same top-level directory.

Dataset Linking also creates local cache artifacts during report processing, so ownership and cleanup of a shared directory should be agreed between the two modules.

## 4. Dataset Profile Input

A local dataset profile is a JSON document containing the fields required by the similarity/refinement logic.

A typical raw profile is:

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

### 4.1 Profile format for `report`

The report logic accepts either a raw dataset object or an object wrapped under `dataset`.

Raw:

```json
{
  "@id": "dataset-a",
  "name": "Dataset A"
}
```

Wrapped:

```json
{
  "dataset": {
    "@id": "dataset-a",
    "name": "Dataset A"
  }
}
```

For meaningful similarity analysis, the profiles should provide usable `keywords`, `description`, and/or `headline` fields.

### 4.2 Profile input for `refine`

In the normal DataGEMS API mode, the upstream caller does not send profile JSON files. It sends the two dataset IDs as `d1` and `d2`, and Dataset Linking retrieves the corresponding Croissant profiles from DataGEMS.

Example:

```text
d1=<dataset-id-1>
d2=<dataset-id-2>
```

In local-folder mode, `refine` reads the selected JSON files directly. The current local refinement path expects the dataset fields at the top level, so local profiles intended for `refine` should use the **raw profile format**, not the `{ "dataset": {...} }` wrapper.

Using raw profiles is therefore the safest format when shared-folder mode is used for both `report` and `refine`.

## 5. Report Job

Use a report job when the upstream module wants Dataset Linking to compare all available profiles.

Endpoint:

```text
POST /api/jobs/report
```

The operation is asynchronous. A successful request creates a job and immediately returns a `job_id`.

### 5.1 Report parameters

| Parameter | Required | Default | Meaning |
|---|---|---:|---|
| `folder` | No | — | Dataset Linking-visible profile folder. Omit it to use DataGEMS API mode. |
| `kw` | No | `0.6` | Keyword contribution |
| `desc` | No | `0.3` | Description contribution |
| `head` | No | `0.1` | Headline contribution |
| `th` | No | `30.0` | Pair similarity threshold, percentage `0..100` |
| `keyword_method` | No | `jaccard` | `jaccard` or `sbert` |
| `include_chunks` | No | `false` | Include description-chunk evidence |
| `include_graphs` | No | `false` | Generate graphs while building the report |
| `graphs_only_above_threshold` | No | `true` | Generate graphs only for pairs passing `th` |

The three similarity weights are normalized if their sum is not `1.0`. Effective weights can also be adjusted per pair when one or more comparable fields are missing.

For an upstream-only integration, `include_graphs=false` is normally sufficient unless graph generation is explicitly required in the same job.

### 5.2 Report — local-folder mode

Example:

```bash
curl -X POST \
  --get "http://127.0.0.1:8000/api/jobs/report" \
  --data-urlencode "folder=/exchange/profiles" \
  --data-urlencode "kw=0.6" \
  --data-urlencode "desc=0.3" \
  --data-urlencode "head=0.1" \
  --data-urlencode "th=30" \
  --data-urlencode "keyword_method=jaccard" \
  --data-urlencode "include_chunks=false" \
  --data-urlencode "include_graphs=false"
```

The folder must exist inside the Dataset Linking runtime and contain at least two usable profiles.

### 5.3 Report — DataGEMS API mode

If `folder` is omitted, Dataset Linking retrieves the available profiles from the configured DataGEMS API.

Example:

```bash
curl -X POST \
  "http://127.0.0.1:8000/api/jobs/report?kw=0.6&desc=0.3&head=0.1&th=30&keyword_method=jaccard&include_chunks=false&include_graphs=false"
```

The upstream caller does not need to provide profile files in this mode, but Dataset Linking itself must have working DataGEMS API configuration and credentials.

### 5.4 Report creation response

Example response:

```json
{
  "job_id": "9fb2...",
  "status": "queued"
}
```

The upstream module should store `job_id` if it needs to verify completion or failure.

## 6. Refine Job

Use a refine job when the upstream module wants detailed Dataset Linking processing for one selected dataset pair.

Endpoint:

```text
POST /api/jobs/refine
```

The preferred platform integration is **DataGEMS API mode**: the caller passes two dataset IDs and Dataset Linking retrieves the corresponding profiles itself. A shared local folder is an alternative integration mode.

### 6.1 Refine parameters

| Parameter | Required | Meaning |
|---|---|---|
| `d1` | Yes | First DataGEMS dataset ID in API mode, or first profile filename in local-folder mode |
| `d2` | Yes | Second DataGEMS dataset ID in API mode, or second profile filename in local-folder mode |
| `folder` | No | Dataset Linking-visible local profile folder. Omit it to use DataGEMS API mode. |

The endpoint also exposes `kw`, `desc`, `head`, and `th` for compatibility with the current API surface. These parameters should not be treated as the stable weighting contract for refinement; the refinement function does not recompute the same weighted pair similarity calculation performed by the report job. Callers should normally omit them unless their use has been explicitly coordinated.

### 6.2 Refine — DataGEMS API mode

When `folder` is omitted, `d1` and `d2` are interpreted as dataset IDs available through the configured DataGEMS API. Dataset Linking retrieves both profiles and performs the refinement.

Example:

```bash
curl -X POST \
  --get "http://127.0.0.1:8000/api/jobs/refine" \
  --data-urlencode "d1=<dataset-id-1>" \
  --data-urlencode "d2=<dataset-id-2>"
```

Example response:

```json
{
  "job_id": "f31a...",
  "status": "queued"
}
```

In this mode the upstream module does not need to create profile files or manage shared storage. It only needs the two platform dataset IDs.

### 6.3 Refine — local-folder mode

When `folder` is provided, `d1` and `d2` refer to the two profile files inside that Dataset Linking-visible directory.

Example:

```bash
curl -X POST \
  --get "http://127.0.0.1:8000/api/jobs/refine" \
  --data-urlencode "folder=/exchange/profiles" \
  --data-urlencode "d1=weather_observations.json" \
  --data-urlencode "d2=climate_indicators.json"
```

The local refinement implementation resolves the files as:

```text
<folder>/<d1>
<folder>/<d2>
```

The files must exist at those paths and should use the raw top-level profile format described above.

### 6.4 Endpoint alignment requirement

The asynchronous `refine` route accepts `folder` as optional. Omitting it enables the DataGEMS-ID mode described above.

## 7. Job Status and Polling

Both `report` and `refine` return a `job_id` and use the same status endpoint:

```text
GET /api/jobs/{job_id}
```

Example:

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
  "progress": 20,
  "message": "Computing similarities...",
  "params": {
    "folder": "/exchange/profiles",
    "kw": 0.6,
    "desc": 0.3,
    "head": 0.1,
    "th": 30.0
  }
}
```

A caller that only needs to trigger Dataset Linking may stop after receiving and storing the `job_id`.

A caller that needs delivery confirmation should poll every few seconds and stop when the job reaches either `completed` or `failed`.

If the status is `failed`, the `message` field contains the job error message.

## 8. Optional Result Endpoints

Retrieving the final result is **not required for the upstream handoff**, but the endpoints are documented here for lifecycle completeness.

### 8.1 JSON result

```text
GET /api/jobs/{job_id}/result
```

If the job is completed, the endpoint returns the completed job result.

If the job is not completed yet, the current implementation returns HTTP `200` with a status payload such as:

```json
{
  "job_id": "9fb2...",
  "status": "in_progress",
  "progress": 20,
  "message": "Computing similarities..."
}
```

It does **not** return `409` in this case.

### 8.2 Download result

```text
GET /api/jobs/{job_id}/download
```

This endpoint returns the completed result as a JSON download.

If the job has not completed yet, `/download` returns HTTP `409`.

## 9. HTTP Errors and Asynchronous Failures

### 9.1 HTTP responses

| Status | Meaning |
|---:|---|
| `200` | Successful API response; for `/result`, this may also represent a job that is still running |
| `400` | Invalid supported parameter or synchronous analysis/API error |
| `404` | Unknown job or resource |
| `409` | `/download` requested before the job completed |
| `422` | FastAPI request validation failed, for example a required query parameter is missing |

### 9.2 Errors after job creation

A job creation request may successfully return `queued` and then fail later during background execution.

Examples include:

- local folder not found;
- insufficient usable profiles;
- selected refine file not found;
- invalid profile content;
- DataGEMS API/authentication failure;
- internal processing exception.

Receiving `queued` means **the job was accepted**, not that processing has completed successfully.

The status endpoint is authoritative for determining whether a queued job became `completed` or `failed`.

## 10. Operational Assumptions

### 10.1 Jobs are stored in process memory

The current implementation stores jobs in an in-memory dictionary inside the Dataset Linking process.

Consequences:

- a service restart removes queued, running, and completed jobs;
- job state is not shared automatically between multiple application workers;
- job state is not shared automatically between multiple service replicas.

For the current integration, run Dataset Linking with a **single application worker/instance** unless sticky routing or an external shared job store has been added.

A future production deployment should move job state to shared persistent infrastructure such as Redis, a database, or a job queue.

### 10.2 POST job creation is not idempotent

Every successful call to a job-creation endpoint creates a new UUID job.

If the upstream caller blindly retries a POST after a network timeout, it may create more than one Dataset Linking job.

The caller should retain successful `job_id` responses and define retry behavior carefully.

### 10.3 Authentication

The current FastAPI application does not define application-level authentication on these job endpoints.

If authentication or authorization is required between project modules, it must be provided by the deployment layer or added to the Dataset Linking API contract.

### 10.4 DataGEMS configuration

DataGEMS API mode depends on Dataset Linking's configured DataGEMS endpoint and credentials. Those are service-side concerns and should not be passed by the upstream caller in each job request.

Before enabling API mode, verify that Dataset Linking can authenticate and retrieve profiles independently from the dataset IDs supplied by the upstream module.

## 11. Recommended Upstream Integration Sequences

### 11.1 Compare all profiles from a shared folder

```text
1. Upstream module writes profile JSON files to shared storage.
2. Upstream module calls POST /api/jobs/report?folder=<shared-path>.
3. Dataset Linking returns {job_id, status:"queued"}.
4. Upstream module stores job_id.
5. If completion confirmation is required, poll GET /api/jobs/{job_id}.
6. Stop polling on completed or failed.
```

### 11.2 Refine one pair already present in DataGEMS

```text
1. Upstream module identifies the two DataGEMS dataset IDs.
2. Upstream module calls POST /api/jobs/refine with d1 and d2, omitting folder.
3. Dataset Linking retrieves both profiles from DataGEMS.
4. Dataset Linking returns {job_id, status:"queued"}.
5. Upstream module stores job_id.
6. If completion confirmation is required, poll GET /api/jobs/{job_id}.
7. Stop polling on completed or failed.
```

### 11.3 Refine one pair from shared local profiles

```text
1. Upstream module ensures both raw profile JSON files are present in shared storage.
2. Upstream module calls POST /api/jobs/refine with folder, d1 and d2.
3. Dataset Linking returns {job_id, status:"queued"}.
4. Upstream module stores job_id.
5. If completion confirmation is required, poll GET /api/jobs/{job_id}.
6. Stop polling on completed or failed.
```

## 12. Integration Checklist for the Upstream Team

Before connecting the modules, confirm the following:

- [ ] The Dataset Linking base URL is known from the upstream runtime.
- [ ] The upstream module knows whether each operation uses DataGEMS API mode or local-folder mode.
- [ ] For DataGEMS `refine`, the upstream module can provide two platform dataset IDs.
- [ ] For local mode, both services can access the same profile storage.
- [ ] The `folder` value is a path valid inside Dataset Linking.
- [ ] Local profiles use raw top-level dataset JSON if they may be passed to `refine`.
- [ ] For local `refine`, `d1` and `d2` are exact JSON filenames.
- [ ] The upstream module stores the `job_id` returned by Dataset Linking.
- [ ] Retry behavior avoids accidental duplicate jobs.
- [ ] The current deployment uses a single Dataset Linking worker/instance, or shared job state is implemented.
- [ ] DataGEMS credentials/configuration are available to Dataset Linking when API mode is used.

## 13. Downstream Output — Out of Scope for This Handoff

The completed jobs produce Dataset Linking report/refinement results and may contain PGJSON graphs.

Those structures are relevant to the module that consumes Dataset Linking output, not to the module that starts Dataset Linking processing.

For the upstream integration described in this document, the required contract ends after:

```text
POST job -> job_id -> optional status polling
```

The downstream PGJSON/graph-registration contract should be documented separately.

## 14. Current Implementation Constraints

These constraints reflect the current API implementation:

1. `report` supports local-folder mode and DataGEMS API mode.
2. `refine` supports DataGEMS-ID mode when `folder` is omitted and local-folder mode when `folder` is provided.
3. In local mode, `refine` treats `d1` and `d2` as filenames and reads `<folder>/<d1>` and `<folder>/<d2>`.
4. Local `refine` expects raw dataset JSON rather than a `{ "dataset": {...} }` wrapper.
5. The `kw`, `desc`, `head`, and `th` parameters exposed by `refine` should not be interpreted as the same weighting contract used by `report`.
6. Job state is in memory and is not safe across restarts, independent workers, or replicas.
7. Job creation has no idempotency key; retries can create duplicate jobs.
8. Shared-folder ownership, cache artifacts, and cleanup behavior should be agreed between modules when local mode is used.

## 15. Relevant Source Files

```text
dl/fastapi_app.py       API routes and asynchronous job lifecycle
dl/similarity.py        Profile loading, pair scores and caching
dl/refine.py            Refinement logic and graph generation
dl/reports.py           Similarity report wrapper
dl/link_comparison.py   Composite and link comparison logic
```
