# Dataset Linking

Web application and REST API for computing semantic links between Croissant-like dataset profiles.

The project provides:

- A Flask UI for interactive analysis, comparison, refinement, and JSON downloads.
- A FastAPI backend for programmatic analysis, report generation, refinement, asynchronous jobs, and PGJSON graph output.
- A similarity engine based on keywords, descriptions, and headlines with configurable weights.
- Refinement logic that inspects file objects, tabular structures, text evidence, and shared resources.

## Quick Start — Installation Guide (Windows 11 / macOS)

This section is intended for users who are new to Python, Git, and the command line. Follow the steps for **your operating system**. You do **not** need Docker, an IDE, or a separate manual installation of Python packages.

**Testing status:** The Windows 11 setup below was successfully tested on a fresh Windows 11 virtual machine (Git, `uv`, Python 3.12, dependency installation, Flask analysis, and FastAPI documentation). The macOS instructions are provided for convenience but **have not yet been tested on a Mac**.

### Windows 11 — Step by step (tested)

**1. Install Git**

Download **Git for Windows** from <https://git-scm.com/downloads/win> and run the installer. The default options are generally sufficient; make sure Git can be used from the command line. Open **PowerShell** (Start menu → search for "PowerShell") and check:

```powershell
git --version
```

You should see a Git version number. If `git` is not recognized, close and reopen PowerShell after installation.

**2. Install `uv`**

In PowerShell, run:

```powershell
winget install --id astral-sh.uv -e
```

Follow any confirmation prompts. Close PowerShell, reopen it, and check:

```powershell
uv --version
```

**3. Install Python 3.12 using `uv`**

```powershell
uv python install 3.12
uv run --no-project --python 3.12 python --version
```

The version command should print `Python 3.12.x`. A warning that Python's executable directory is not on `PATH` is not a problem for this setup: `uv` will use the Python version it installed.

**4. Download the project**

Choose where to keep the project. For example, to create a `Projects` folder in your user directory:

```powershell
cd $HOME
mkdir Projects
cd Projects
git clone https://github.com/datagems-eosc/dataset-linking.git
cd dataset-linking
```

All following commands must be run **inside the `dataset-linking` directory**, where `pyproject.toml` and `uv.lock` are located. If `Projects` already exists, the `mkdir` step can be skipped.

**5. Install project dependencies**

```powershell
uv sync --python 3.12 --locked
```

This creates or updates the project's `.venv` virtual environment and installs the dependencies from the lock file. The first installation may take a while, particularly for machine-learning libraries. Do not close the terminal until the command finishes without errors.

**6. Start the web application (Flask)**

```powershell
uv run flask --app dl.flask_app run
```

Keep this terminal open. Inside the same computer, open <http://127.0.0.1:5000> in a browser. For a first test, enter `demo_profiles` in the folder field and run an analysis. Initial model downloads or computations may take extra time.

**7. Start the REST API (FastAPI; optional)**

Open a **second** PowerShell window and navigate to your cloned project directory again (for the example above, use `cd "$HOME\Projects\dataset-linking"`). Then run:

```powershell
uv run uvicorn dl.fastapi_app:app --host 127.0.0.1 --port 8000
```

Keep the second terminal open and visit <http://127.0.0.1:8000/docs> for the interactive API documentation (Swagger UI). You can leave Flask running in the first terminal.

To stop either server, return to its terminal and press **Ctrl+C**.

### macOS — Step by step (not yet verified on a Mac)

These commands are intended for a regular macOS Terminal. Some details can differ by macOS version and whether your Mac has an Apple Silicon or Intel processor.

**1. Install Git**

Open **Terminal** (Applications → Utilities → Terminal) and check:

```bash
git --version
```

If Git is unavailable, macOS may offer to install Apple's Command Line Tools. You can also request them with:

```bash
xcode-select --install
```

Follow the on-screen instructions and then check `git --version` again.

**2. Install `uv`**

Follow the official instructions at <https://docs.astral.sh/uv/getting-started/installation/>. One supported installation method is:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Close and reopen Terminal, then check:

```bash
uv --version
```

If `uv` cannot be found, follow the PATH instructions displayed by its installer or on the official installation page.

**3. Install Python 3.12 and clone the project**

```bash
uv python install 3.12
uv run --no-project --python 3.12 python --version
mkdir -p ~/Projects
cd ~/Projects
git clone https://github.com/datagems-eosc/dataset-linking.git
cd dataset-linking
```

**4. Install dependencies**

```bash
uv sync --python 3.12 --locked
```

**5. Run Flask**

```bash
uv run flask --app dl.flask_app run
```

Open <http://127.0.0.1:5000> and try the `demo_profiles` folder. Keep Terminal open while using the application.

**6. Run FastAPI (optional)**

Open a second Terminal window:

```bash
cd ~/Projects/dataset-linking
uv run uvicorn dl.fastapi_app:app --host 127.0.0.1 --port 8000
```

Open <http://127.0.0.1:8000/docs>. Stop either server by pressing **Ctrl+C** in the corresponding Terminal window.

### Common Issues

- **`git` or `uv` is not recognized / command not found:** close and reopen your terminal; make sure the tool was installed successfully. For `uv` on macOS, check the installer PATH instructions.
- **`python` is not recognized:** use `uv` commands as shown above; Python does not need to be on your global `PATH`.
- **`uv sync` fails:** confirm you are inside `dataset-linking` and have an Internet connection. Copy the complete error message when requesting help; do not blindly install packages one by one.
- **Browser cannot reach Flask or FastAPI:** confirm that the corresponding terminal is still running, and open the matching URL and port (`5000` for Flask, `8000` for FastAPI).
- **First analysis is slow:** the application uses embedding models that may be downloaded or initialized on first use. Wait for the analysis to finish and check the server terminal for errors.
- **Using DataGEMS platform endpoints:** some remote/API-based workflows may need access or configuration beyond the local `demo_profiles` example. The quick start only verifies local operation.

---

## Project Layout

```text
dl/                         Application and analysis modules
templates/                  Flask templates
static/                     Browser assets
tests/                      Automated tests
scripts/demos/              Small demonstrations
scripts/datagems/           DataGEMS download and cleanup utilities
scripts/benchmarks/         Performance benchmarks
scripts/language_use_case/  Language use-case utilities
data/                       Downloaded profiles and local runtime data
reports/                    Generated reports and benchmark documents
docs/                       API and integration documentation
```

Bulk downloaded profiles and benchmark inputs are stored under `data/`; the
legacy `dl/profiles_api/` folder is kept because it is part of the existing
application image. Neither is required to run the application with
`demo_profiles/`.

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

## Demo Profiles

The repository includes ten small, synthetic profiles in `demo_profiles/`, arranged in two thematic groups. To run them locally, enter `demo_profiles` in the folder field. Demo data uses a 45% threshold to make the two intended final composites easier to see without calling the DataGEMS API; the general application default remains unchanged.

To demonstrate the multi-profile chunk evidence directly, run:

```bash
   uv run python scripts/demos/demo_multi_profile_chunks.py
```

The script prints up to three evidence groups for each thematic group, with one compatible description chunk per profile.

During composite construction, each original profile contributes at most ten deduplicated candidate chunks, selected by length. Their embeddings and pairwise chunk-similarity matrices are cached and reused at later levels. The top three multi-profile chunk groups are stored as output evidence; they are not used as the only source for later levels.

For a focused four-profile explanation, run:

```bash
   uv run python scripts/demos/demo_four_profile_chunks.py
```

This script evaluates every candidate combination of chunks for profiles A, B, C and D. It checks all six pairwise similarities (`AB`, `AC`, `AD`, `BC`, `BD`, `CD`) and prints the top three four-chunk evidence groups with their minimum, average and combined scores.

When building composites, the terminal shows progress such as the number of base pairs, cached profiles, green/red items per level, and final terminal composites. Results computed on the home page are reused by the composite request when the source and parameters are unchanged.

## Run The FastAPI App

From the project root:

```bash
uv run uvicorn dl.fastapi_app:app --host 0.0.0.0 --port 8000 --reload
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

For the complete integration contract, asynchronous job examples, polling workflow, result shapes, error codes, and PGJSON extraction instructions, see [`docs/API_INTEGRATION.md`](docs/API_INTEGRATION.md).

Main endpoints:

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/` | Health/root response |
| `GET` | `/api/similarities` | Compute all pairwise similarities |
| `GET` | `/api/similarity/single` | Return one pair similarity with optional pair chunk evidence |
| `GET` | `/api/report` | Build a report JSON |
| `GET` | `/api/report/download` | Download a report JSON |
| `GET` | `/api/report/pair/download` | Download a single pair report |
| `POST` | `/api/compare-links` | Compare DataLinkingBase links |
| `GET` | `/api/composites` | Build composite DataLinking elements |
| `GET` | `/api/composites/download` | Download a MoMa-compatible composite export |
| `GET` | `/api/refine` | Run refinement for one pair and return graph data |
| `GET` | `/api/refine/download` | Download one refinement report |
| `POST` | `/api/similarities/select` | Compute similarities for selected profiles |
| `POST` | `/api/jobs/report` | Start an async report job |
| `POST` | `/api/jobs/refine` | Start an async refine job |
| `GET` | `/api/jobs/{job_id}` | Poll job status |
| `GET` | `/api/jobs/{job_id}/result` | Get completed job result |
| `GET` | `/api/jobs/{job_id}/download` | Download completed job result |
| `GET` | `/api/discover-and-compare` | Fetch platform profiles and compare them |

Similarity, report, comparison, and composite endpoints accept
`force_refresh=true` to ignore report, pairwise, embedding, and composite
chunk caches and recompute the analysis from scratch.

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

## Composite Builds

Composite construction is implemented in `dl/link_comparison.py` and is used by
both `dl/flask_app.py` and `dl/fastapi_app.py`. The build has three distinct
stages:

1. Base links are filtered using the requested base similarity threshold.
2. Green links are merged into profile groups. Smaller inputs use the
   level-by-level recursive hierarchy and preserve the green-parent rule.
3. Terminal groups receive aggregated chunk evidence and are returned as
   `CompositeDataLinkingElement` objects.

The composite metrics have separate meanings:

- `composite_coherence` is the final recursive group score.
- `chunk_similarity`/`chunk_similarity_avg` measure description evidence.
- `keyword_similarity` measures keyword overlap across the internal profile
  links.
- `multi_profile_chunk_matches` contains the strongest one-chunk-per-profile
  evidence groups shown in the UI.

Chunk evidence is deliberately bounded. At most ten deduplicated chunks are
considered per profile. For groups up to twenty profiles, candidate groups are
searched exhaustively when possible and otherwise with a bounded beam search.
For larger groups, one representative group is selected against a global
semantic centroid; this avoids a combinatorial explosion while preserving a
readable explanation for large composites. Near-duplicate evidence groups are
not displayed repeatedly.

For dense inputs with more than 1,000 base links, recursive clique enumeration
is replaced by a connected-component pass with bounded processing. Only base
links above the effective base threshold participate, and components below the
relation threshold are not returned as final green composites. Since an
all-pairs run over 100 profiles produces 4,950 base links, this path is used
for datasets of that size and larger dense graphs.

The composite route prints progress information to the server log. A long pause
after `Profile evidence ready` usually indicates that an old server process is
still running; restart Flask/FastAPI after code changes before repeating the
request. Use `force_refresh=true` only when intentionally invalidating the
analysis and embedding caches.

## Handoff Checklist

Run the following from the repository root before changing composite logic:

```bash
uv run python -m unittest discover -s tests -v
python -m py_compile dl/link_comparison.py dl/flask_app.py dl/fastapi_app.py
git diff --check
```

The main regression coverage is in `tests/test_link_comparison.py`. It covers
original chunk registries, cached profile-pair evidence, recursive merges,
large chunk candidate sets, beam-search inputs, and terminal composite evidence
propagation. The benchmark and demo scripts under `scripts/benchmarks/` and
`scripts/demos/` are useful for checking performance and evidence quality
without calling the production API.

## Caching

Result and embedding caches are used to avoid repeated expensive computations.

API mode uses the persistent cache location:

```text
/s3/cache
```

Local mode stores cache data close to the selected local folder.

Cache fingerprints include source signatures, weights, local file content fingerprints, and scoring-mode versions where relevant.

## Utility Scripts

Useful scripts are grouped under `scripts/`:

- `scripts/datagems/`: download and deduplication utilities.
- `scripts/benchmarks/`: performance benchmarks.
- `scripts/demos/`: small composite-linking demonstrations.
- `scripts/language_use_case/`: language use-case analysis and export utilities.
- `dl/count_datasets.py`, `dl/export_excel.py`, and `dl/run_refinement.py`: legacy diagnostics and helpers.

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

scripts/
├── benchmarks/
├── datagems/
├── demos/
└── language_use_case/

data/
├── datagems/
└── language_use_case/

reports/
├── benchmarks/
├── language_use_case/
└── legacy/
```

## Models

- `all-MiniLM-L6-v2`: short text embeddings, mainly headline scoring.
- `all-mpnet-base-v2`: long text embeddings, mainly description scoring and chunk evidence.

Models are loaded lazily when first needed.

## Notes

- The temporary branch `temp/sbert-keywords-local-analysis` contains an experimental local-analysis variant where keyword similarity is SBERT-based. It is not part of the stable `main` behavior described here.
- Generated files such as `__pycache__`, local caches, downloaded profiles, and generated analysis results are kept under `data/` or `reports/` and ignored unless explicitly needed.

## Author

Developed by Stefano Tanfoglio for the Master's Thesis Project - Data Linking, University of Verona, 2026.
