# dl/refine.py
"""
Phase 2 - Refinement module
--------------------------------
Given two DataProfiles (JSON files), perform a deeper structural analysis:

1. Inspect `distribution` to understand:
       - folders (FileSet) vs files (FileObject)
       - resource formats (text, csv, SQL, etc.)
2. Infer dataset content type:
       - TEXTUAL -> only plain text resources
       - CSV     -> only csv/tabular resources
       - SQL     -> presence of SQL resources
       - MIXED   -> mix of different formats
       - UNKNOWN -> nothing detectable
3. Build a structural view:
       - text documents (from FileSet text/plain + dg:Document)
         including their names and document keywords (if present)
       - csv tables and their column names
4. Compare datasets:
       - TXT: compare document names + keyword overlaps
       - CSV: compare column names
5. Return a structured refinement report.
"""

import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import timezone, datetime


# --------------------------------------------------------------------
# Utility: name similarity (reserved for future extensions)
# --------------------------------------------------------------------
def simple_name_similarity(name1: str, name2: str) -> float:
    """
    Very simple token-based similarity for names.
    Returns a value in [0, 1].
    """
    if not name1 or not name2:
        return 0.0

    tokens1 = set(name1.replace("_", " ").lower().split())
    tokens2 = set(name2.replace("_", " ").lower().split())

    common = tokens1 & tokens2
    union = tokens1 | tokens2

    return len(common) / len(union) if union else 0.0


# --------------------------------------------------------------------
# 1) Distribution helpers
# --------------------------------------------------------------------
def _guess_format_from_paths(dist: Dict[str, Any]) -> Optional[str]:
    """
    Fallback heuristic when encodingFormat is missing.
    Tries to infer a mime-like type from file extensions found in:
        - includes
        - contentUrl
        - name
    """
    joined = " ".join(
        [
            (dist.get("includes") or "").lower(),
            (dist.get("contentUrl") or "").lower(),
            (dist.get("name") or "").lower(),
        ]
    )

    if ".csv" in joined:
        return "text/csv"
    if ".txt" in joined:
        return "text/plain"
    if ".sql" in joined:
        return "application/sql"
    if ".pdf" in joined:
        return "application/pdf"
    if ".xls" in joined or ".xlsx" in joined:
        return "application/vnd.ms-excel"

    return None


def analyze_distribution(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inspect `distribution` and produce a structured summary:
    - distinguish folders (FileSet) from files (FileObject)
    - expose encodingFormat / includes / contentUrl
    """
    raw = profile_data.get("distribution", []) or []
    items: List[Dict[str, Any]] = []

    for dist in raw:
        cr_type = (dist.get("@type") or "").strip()
        encoding = dist.get("encodingFormat") or _guess_format_from_paths(dist)

        if cr_type.endswith("FileSet"):
            kind = "folder"
        elif cr_type.endswith("FileObject"):
            kind = "file"
        else:
            kind = "other"

        items.append(
            {
                "id": dist.get("@id"),
                "name": dist.get("name"),
                "kind": kind,
                "croissant_type": cr_type,
                "encodingFormat": encoding,
                "includes": dist.get("includes") if kind == "folder" else None,
                "contentUrl": dist.get("contentUrl"),
            }
        )

    return {
        "total": len(items),
        "folders": sum(1 for i in items if i["kind"] == "folder"),
        "files": sum(1 for i in items if i["kind"] == "file"),
        "other": sum(1 for i in items if i["kind"] == "other"),
        "items": items,
    }


# --------------------------------------------------------------------
# 2) Content classification (TEXTUAL / CSV / SQL / MIXED / UNKNOWN)
# --------------------------------------------------------------------
def infer_content_type(profile_data: Dict[str, Any]) -> str:
    """
    Content-driven classification based on distribution contents:

       TEXTUAL -> only plain text resources (text/plain, txt)
       CSV     -> only csv/tabular resources (text/csv, excel-like)
       SQL     -> presence of SQL resources (takes precedence)
       MIXED   -> combination of multiple heterogeneous formats
       UNKNOWN -> no distribution found
    """
    summary = analyze_distribution(profile_data)
    items = summary["items"]

    if not items:
        return "UNKNOWN"

    has_text = False
    has_csv = False
    has_sql = False
    has_other = False

    for it in items:
        encoding = (it.get("encodingFormat") or "").lower()

        if not encoding:
            has_other = True
        elif encoding.startswith("text/") and encoding != "text/csv":
            has_text = True
        elif encoding == "text/csv" or "excel" in encoding:
            has_csv = True
        elif "sql" in encoding:
            has_sql = True
        else:
            has_other = True

    if has_text and not has_csv and not has_sql and not has_other:
        return "TEXTUAL"

    if has_csv and not has_text and not has_sql and not has_other:
        return "CSV"

    if has_sql:
        return "SQL"

    return "MIXED"


# --------------------------------------------------------------------
# 3) Extract structural info: TXT (documents + keywords) and CSV
# --------------------------------------------------------------------
def extract_txt_documents(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract text documents connected to FileSet resources (text/plain).

    This function also extracts Document-level keywords if present.

    Returns:
        {
          "file_sets": { fileSet_id: { ... }, ... },
          "documents_by_file_set": {
              fileSet_id: [
                  {"name": "MANIFEST", "name_norm": "manifest", "keywords": ["..."]},
                  ...
              ],
              ...
          },
          "all_document_names": ["manifest", "readme", ...],  # lowercase unique
          "all_document_keywords": ["integration", "calculus", ...]  # lowercase unique
        }
    """
    txt_sets: Dict[str, Dict[str, Any]] = {}

    for dist in profile_data.get("distribution", []) or []:
        cr_type = (dist.get("@type") or "").strip()
        if not cr_type.endswith("FileSet"):
            continue

        encoding = dist.get("encodingFormat") or _guess_format_from_paths(dist)
        encoding = (encoding or "").lower()

        if encoding != "text/plain":
            continue

        fs_id = dist.get("@id")
        if fs_id:
            txt_sets[fs_id] = {
                "id": fs_id,
                "name": dist.get("name"),
                "includes": dist.get("includes"),
                "contentUrl": dist.get("contentUrl"),
            }

    # Store full document objects per fileset
    documents_by_fs: Dict[str, List[Dict[str, Any]]] = {k: [] for k in txt_sets}

    for recordset in profile_data.get("recordSet", []) or []:
        for field in recordset.get("field", []) or []:
            if not (field.get("@type") or "").endswith("Document"):
                continue

            source = field.get("source", {}) or {}
            fs = source.get("fileSet", {}) or {}
            fs_id = fs.get("@id")

            if fs_id in txt_sets:
                doc_name = (field.get("name") or "").strip()
                if not doc_name:
                    continue

                kws = field.get("keywords") or []
                if isinstance(kws, str):
                    kws = [kws]

                kws_norm = sorted(
                    {
                        str(k).strip().lower()
                        for k in kws
                        if str(k).strip()
                    }
                )

                documents_by_fs[fs_id].append(
                    {
                        "name": doc_name,
                        "name_norm": doc_name.lower(),
                        "keywords": kws_norm,
                    }
                )

    all_names = sorted(
        {
            d.get("name_norm")
            for docs in documents_by_fs.values()
            for d in docs
            if d.get("name_norm")
        }
    )

    all_keywords = sorted(
        {
            kw
            for docs in documents_by_fs.values()
            for d in docs
            for kw in (d.get("keywords") or [])
        }
    )

    return {
        "file_sets": txt_sets,
        "documents_by_file_set": documents_by_fs,
        "all_document_names": all_names,
        "all_document_keywords": all_keywords,
    }


def extract_csv_tables_with_samples(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extended CSV extraction:
    - column names
    - per-column sample values (if present)
    """
    csv_sources: Dict[str, Dict[str, Any]] = {}

    for dist in profile_data.get("distribution", []) or []:
        encoding = dist.get("encodingFormat") or _guess_format_from_paths(dist)
        encoding = (encoding or "").lower()

        if encoding == "text/csv" or "excel" in encoding:
            src_id = dist.get("@id")
            if src_id:
                csv_sources[src_id] = {
                    "id": src_id,
                    "name": dist.get("name") or src_id,
                    "columns": {},
                }

    if not csv_sources:
        return {"tables": [], "all_columns": []}

    for recordset in profile_data.get("recordSet", []) or []:
        for field in recordset.get("field", []) or []:
            source = field.get("source", {}) or {}
            ref = source.get("fileSet") or source.get("fileObject") or {}
            src_id = ref.get("@id")

            if src_id in csv_sources:
                col = (field.get("name") or "").strip().lower()
                if not col:
                    continue

                samples = field.get("sample") or []
                if not isinstance(samples, list):
                    samples = [samples]

                samples_norm = sorted(
                    {
                        str(s).strip().lower()
                        for s in samples
                        if str(s).strip()
                    }
                )

                csv_sources[src_id]["columns"].setdefault(col, []).extend(samples_norm)

    tables = []
    for t in csv_sources.values():
        for c in t["columns"]:
            t["columns"][c] = sorted(set(t["columns"][c]))
        tables.append(t)

    all_columns = sorted({c for t in tables for c in t["columns"].keys()})

    return {
        "tables": tables,
        "all_columns": all_columns,
    }



# --------------------------------------------------------------------
# 4) Comparisons
# --------------------------------------------------------------------
def compare_txt_files(txt1: Dict[str, Any], txt2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare text documents by name (case-insensitive), plus keyword overlaps:
      - global keyword overlap across all Documents
      - per-document overlap when document names match
    """
    names1 = set(txt1.get("all_document_names") or [])
    names2 = set(txt2.get("all_document_names") or [])
    common_names = sorted(names1 & names2) if names1 and names2 else []

    kw1 = set(txt1.get("all_document_keywords") or [])
    kw2 = set(txt2.get("all_document_keywords") or [])
    common_kw = sorted(kw1 & kw2) if kw1 and kw2 else []

    def _name_to_keywords(txt_info: Dict[str, Any]) -> Dict[str, set]:
        out: Dict[str, set] = {}
        docs_by_fs = txt_info.get("documents_by_file_set") or {}
        for docs in docs_by_fs.values():
            for d in docs:
                name_norm = d.get("name_norm")
                if not name_norm:
                    continue
                out.setdefault(name_norm, set()).update(d.get("keywords") or [])
        return out

    map1 = _name_to_keywords(txt1)
    map2 = _name_to_keywords(txt2)

    per_doc_overlap: List[Dict[str, Any]] = []
    for n in common_names:
        k1 = map1.get(n, set())
        k2 = map2.get(n, set())
        per_doc_overlap.append(
            {
                "document_name": n,
                "common_keywords": sorted(k1 & k2),
                "dataset1_keywords": sorted(k1),
                "dataset2_keywords": sorted(k2),
            }
        )

    return {
        "dataset1_document_names": sorted(names1),
        "dataset2_document_names": sorted(names2),
        "common_document_names": common_names,
        "dataset1_document_keywords": sorted(kw1),
        "dataset2_document_keywords": sorted(kw2),
        "common_document_keywords": common_kw,
        "per_document_keyword_overlap": per_doc_overlap,
    }


def compare_csv_schemas_with_samples(csv1: Dict[str, Any], csv2: Dict[str, Any]) -> Dict[str, Any]:
    cols1 = set(csv1.get("all_columns") or [])
    cols2 = set(csv2.get("all_columns") or [])

    common_cols = sorted(cols1 & cols2) if cols1 and cols2 else []

    def _build_sample_map(csv_struct: Dict[str, Any]) -> Dict[str, set]:
        out = {}
        for table in csv_struct.get("tables", []):
            for column, samples in table.get("columns", {}).items():
                out.setdefault(column, set()).update(samples)
        return out

    map1 = _build_sample_map(csv1)
    map2 = _build_sample_map(csv2)

    per_column_overlap = []
    for col in common_cols:
        s1 = map1.get(col, set())
        s2 = map2.get(col, set())
        per_column_overlap.append(
            {
                "column": col,
                "dataset1_samples": sorted(s1),
                "dataset2_samples": sorted(s2),
                "common_samples": sorted(s1 & s2),
            }
        )

    return {
        "dataset1_columns": sorted(cols1),
        "dataset2_columns": sorted(cols2),
        "common_columns": common_cols,
        "per_column_sample_overlap": per_column_overlap,
    }



# --------------------------------------------------------------------
# 5) Main refinement pipeline
# --------------------------------------------------------------------
def refine_similarity(
    folder_path: str,
    dataprofile1: str,
    dataprofile2: str,
    _kw_weight: float = 0.6,   # kept for backward compatibility (not used)
    _desc_weight: float = 0.3, # kept for backward compatibility (not used)
    _head_weight: float = 0.1, # kept for backward compatibility (not used)
    _threshold: float = 30.0,  # kept for backward compatibility (not used)
) -> Dict[str, Any]:
    """
    Main refinement entrypoint.

    Steps:
        1. Load both profiles
        2. Infer content type (TEXTUAL / CSV / SQL / MIXED / UNKNOWN)
        3. Inspect distribution (folders/files)
        4. Extract TXT and CSV structure (including TXT Document keywords)
        5. Compare text document names + keywords and csv column names
    """
    folder = Path(folder_path)
    file1 = folder / dataprofile1
    file2 = folder / dataprofile2

    if not file1.exists() or not file2.exists():
        return {"error": f"One or both files not found: {file1} / {file2}"}

    dp1 = json.loads(file1.read_text(encoding="utf-8"))
    dp2 = json.loads(file2.read_text(encoding="utf-8"))

    content_type1 = infer_content_type(dp1)
    content_type2 = infer_content_type(dp2)

    dist1 = analyze_distribution(dp1)
    dist2 = analyze_distribution(dp2)

    txt1 = extract_txt_documents(dp1)
    txt2 = extract_txt_documents(dp2)
    txt_cmp = compare_txt_files(txt1, txt2)

    csv1 = extract_csv_tables_with_samples(dp1)
    csv2 = extract_csv_tables_with_samples(dp2)
    csv_cmp = compare_csv_schemas_with_samples(csv1, csv2)

    notes: List[str] = [
        f"Dataset1 content type: {content_type1}.",
        f"Dataset2 content type: {content_type2}.",
    ]

    # TXT: name overlap
    if txt_cmp.get("common_document_names"):
        notes.append(
            f"TXT: found {len(txt_cmp['common_document_names'])} common document names."
        )

    # TXT: global keyword overlap
    if txt_cmp.get("common_document_keywords"):
        notes.append(
            f"TXT: found {len(txt_cmp['common_document_keywords'])} common document keywords."
        )

    # TXT: per-document keyword overlap (for same-name docs)
    per_doc = txt_cmp.get("per_document_keyword_overlap") or []
    per_doc_with_overlap = sum(1 for x in per_doc if x.get("common_keywords"))
    if per_doc_with_overlap > 0:
        notes.append(
            f"TXT: keyword overlap detected for {per_doc_with_overlap} common-named documents."
        )

    # CSV: columns overlap
    if csv_cmp.get("common_columns"):
        notes.append(
            f"CSV: found {len(csv_cmp['common_columns'])} common column names."
        )

    if (
        not txt_cmp.get("common_document_names")
        and not txt_cmp.get("common_document_keywords")
        and not csv_cmp.get("common_columns")
    ):
        notes.append("No clear structural similarity found (TXT or CSV).")

    return {
        "dataprofile1": dataprofile1,
        "dataprofile2": dataprofile2,
        "dataprofile1_content_type": content_type1,
        "dataprofile2_content_type": content_type2,
        "distribution_dataset1": dist1,
        "distribution_dataset2": dist2,
        "txt_structure_dataset1": txt1,
        "txt_structure_dataset2": txt2,
        "csv_structure_dataset1": csv1,
        "csv_structure_dataset2": csv2,
        "txt_comparison": txt_cmp,
        "csv_schema_comparison": csv_cmp,
        "note": " ".join(notes),
    }


def build_refinement_profile(report: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build an external-facing refinement profile (Croissant-like)
    from the internal refinement report produced by refine_similarity().
    """
    dp1 = report.get("dataprofile1")
    dp2 = report.get("dataprofile2")

    content_type1 = report.get("dataprofile1_content_type")
    content_type2 = report.get("dataprofile2_content_type")

    dist1 = report.get("distribution_dataset1") or {}
    dist2 = report.get("distribution_dataset2") or {}

    txt1 = report.get("txt_structure_dataset1") or {}
    txt2 = report.get("txt_structure_dataset2") or {}

    csv1 = report.get("csv_structure_dataset1") or {}
    csv2 = report.get("csv_structure_dataset2") or {}

    txt_cmp = report.get("txt_comparison") or {}
    csv_cmp = report.get("csv_schema_comparison") or {}

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    refinement_profile: Dict[str, Any] = {
        "@type": "RefinementReport",
        "name": f"Refinement between {dp1} and {dp2}",
        "generatedAtTime": now_iso,
        "datasets": [
            {
                "@id": dp1,
                "contentType": content_type1,
                "distributionSummary": {
                    "total": dist1.get("total"),
                    "folders": dist1.get("folders"),
                    "files": dist1.get("files"),
                    "formats": sorted(
                        {
                            (item.get("encodingFormat") or "").lower()
                            for item in dist1.get("items", [])
                            if item.get("encodingFormat")
                        }
                    ),
                },
                "structure": {
                    "textDocuments": txt1.get("all_document_names") or [],
                    "textDocumentKeywords": txt1.get("all_document_keywords") or [],
                    "csvColumns": csv1.get("all_columns") or [],
                },
            },
            {
                "@id": dp2,
                "contentType": content_type2,
                "distributionSummary": {
                    "total": dist2.get("total"),
                    "folders": dist2.get("folders"),
                    "files": dist2.get("files"),
                    "formats": sorted(
                        {
                            (item.get("encodingFormat") or "").lower()
                            for item in dist2.get("items", [])
                            if item.get("encodingFormat")
                        }
                    ),
                },
                "structure": {
                    "textDocuments": txt2.get("all_document_names") or [],
                    "textDocumentKeywords": txt2.get("all_document_keywords") or [],
                    "csvColumns": csv2.get("all_columns") or [],
                },
            },
        ],
        "comparisons": {
            "text": {
                "commonDocumentNames": txt_cmp.get("common_document_names") or [],
                "commonDocumentKeywords": txt_cmp.get("common_document_keywords") or [],
                "perDocumentKeywordOverlap": txt_cmp.get("per_document_keyword_overlap") or [],
            },
            "csv": {
                "commonColumns": csv_cmp.get("common_columns") or [],
                "perColumnSampleOverlap": csv_cmp.get("per_column_sample_overlap") or [],
            },
        },
        "summary": report.get("note"),
    }

    return refinement_profile
