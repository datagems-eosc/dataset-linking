# 🧩 Profile Similarity Web App

A modular web application for analyzing and comparing JSON profile datasets.  
It computes semantic similarities between profiles based on **keywords**, **descriptions**, and **headlines**, using transformer models and configurable weights. 

The system support a **two-phase analysis**:
1. **Metadata-based similarity computation**
2. **Refinement analyzing dataset structure and resource types**

Now includes both:
- 🧠 **Flask interface** for browser-based visualization  
- ⚙️ **FastAPI backend** for programmatic access via REST APIs

---

## 🔍 Features

### Phase 1 - Dataset Similarity
- Similarity computation based on:
  - keywords
  - descriptions
  - headlines
- Adjustable similarity weights
- Automatic weight normalization when the sum exceeds 1
- Configurable similarity threshold
- Automatic caching to avoid redundant computations
- Export of:
  - full similarity results
  - single dataset-pair results  
  as Croissant-like JSON

### Phase 2 - Refinement
- Inspection of dataset structure via `distribution` metadata
- Detection of:
  - folders (FileSet)
  - files (FileObject)
  - resource formats (CSV, TXT, PDF, SQL, etc.)
- Content-type inference:
  - TEXTUAL
  - CSV
  - SQL
  - MIXED
- Structural comparison of:
  - CSV schemas (column names)
  - text documents (document names)
  - document-level keywords
- Detection of keyword overlap across datasets
- Generation of a human-readable refinement summary
- Export of a Croissant-like refinement JSON report

---

## 🧩 Requirements

- Python 3.9+
- pip

> The application has been tested on Windows using Python 3.12

---

## ⚙️ Installation and how to Run

> Note: Git is recommended to clone the repository, but the project can also be downloaded as a ZIP archive.

Clone the repository and move into the project folder:
```bash
git clone <repository-url>
cd <repository-folder>
```

Create and activate a virtual environment
```bash
python -m venv venv
venv\Scripts\Activate
```

Install the required dependencies:
```bash
pip install flask fastapi uvicorn sentence-transformers torch numpy scipy scikit-learn
```
📁 Input Data

The application expects a folder containing Croissant DataProfiles in JSON format.

Example:
```
Profiles/
├── dataset1.json
├── dataset2.json
├── dataset3.json
```
By default, the application looks for profiles in: **C:\Users\<username>\Desktop\Profiles**

### ▶️ Running the Web Application
To start the flask web interface, run:
```bash
python -m app.main
```
If the application starts correctly, the terminal will display **Running on http://127.0.0.1:5000**.
Now Open a web browser and navigate to **http://127.0.0.1:5000**.

🖥️ Web Interface Overview

From the main interface, users can:

- Select the folder containing the DataProfiles
- Configure similarity weights and threshold
- Run the similarity analysis
- Inspect pairwise similarity results
- Export similarity reports as JSON

If the sum of similarity weights exceeds 1, the system automatically normalizes the values to ensure a valid configuration.

---

### ⚙️ FastAPI Backend

The project also exposes a full REST API implemented with **FastAPI**, suitable for
programmatic access, integration with external systems, and long-running batch jobs.

Start the API server with:

```bash
python -m uvicorn app.fastapi_app:app --reload
```

Then open the interactive documentation at:
```
http://127.0.0.1:8000/docs
```

Available endpoints:
| Endpoint | Description |
|-----------|-------------|
| `/api/similarities` | Compute all pairwise similarities |
| `/api/similarity/single` | Compute similarity between two profiles |
| `/api/report` | Build a Croissant-style similarity report (JSON) |
| `/api/report/download` | Download the full similarity report as a JSON file |
| `/api/report/pair/download` | Download a single-pair similarity report |
| `/api/refine` | Perform structural refinement between two profiles |
| `/api/refine/dowload` | Download the refinement report as a JSON file |
| `/api/similarities/select` | Compute similarities only for a subset of profiles |

---

## ⏳ Asynchronous Jobs (Long-running Operations
Some operations (such as computing similarities on large profile collections or performing structural refinement) may take a significant amount of time.
To avoid client timeouts and to enable progress monitoring, the FastAPI backend supports asynchronous job-based execution.

In this mode, a request starts a background job and immediately returns a job_id.
The client can then poll the job status, retrieve the result when completed, or download it as a file.

- POST /api/jobs/report (Start an asynchronous job to compute the full similarity report for a folder)
- POST /api/jobs/refine (Start an asynchronous job to perform structural refinement between two profiles)
- GET /api/jobs/{job_id} (Retrieve the current status and progress of a job)
- GET /api/jobs/{job_id}/result (Retrieve the job result as a JSON response (available only when the job is completed)
- GET /api/jobs/{job_id}/download (Download the job result as a JSON file (available only when the job is completed))


## 📁 Project Structure

```
app/
├── main.py          → Entry point (launches Flask app)
├── flask_app.py     → Flask routes and web interface
├── fastapi_app.py   → FastAPI routes and REST endpoints
├── similarity.py    → Core similarity computations
├── reports.py       → Croissant-style report builder
└── utils.py         → Helper functions and cache management

templates/           → Web UI (index.html)
static/              → CSS styling
cache/               → Cached similarity results
```

---

## 🧠 Models Used
- `all-MiniLM-L6-v2` → embeddings for **headlines** (fast, lightweight)  
- `all-mpnet-base-v2` → embeddings for **descriptions** (high-quality)

These models are **lazily loaded** — they are initialized only when first needed, reducing startup time and memory usage.

---

## 🧾 Example Output
### 📝 TODO

---

## 📚 How It Works Internally

1. **Input Loading** — reads all `.json` profiles from the selected folder.  
2. **Normalization** — cleans and lowercases all keyword lists.  
3. **Embedding** — encodes descriptions and headlines using transformer models.  
4. **Computation** — combines Jaccard and cosine similarities with weighted scoring.  
5. **Export** — generates a semantic Croissant-style JSON report with `DLElement` and `SimilarityLink`.

---



## 👨‍💻 Author
Developed by **Stefano Tanfoglio**  
for the *Master’s Thesis Project — Data Linking*  
(University of Verona, 2026).
