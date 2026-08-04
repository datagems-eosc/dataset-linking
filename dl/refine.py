# dl/refine.py
import json
import re
import uuid
import sys
import os
import torch
from pathlib import Path
from typing import Dict, Any, List, Set, Optional
from datetime import timezone, datetime
from sentence_transformers import util

# --- IMPORT MODULI LOCALI ---
from dl.utils import normalize_keywords
from dl import similarity


# --------------------------------------------------------------------
# 1) Explainability
# --------------------------------------------------------------------
def split_chunks(text: str) -> List[str]:
    if not text or len(str(text)) < 20:
        return []

    chunks = re.split(r"[,.:;]| and | or | but ", str(text))

    clean_chunks = []
    for c in chunks:
        c = c.strip()
        # filter: almost 3 words and 20 chars
        if len(c.split()) >= 3 and len(c) > 20:
            clean_chunks.append(c)

    return clean_chunks


def normalize_name(name: str) -> str:
    if not name: return ""
    name_no_ext = os.path.splitext(str(name))[0]
    return re.sub(r'[_ \-]', '', name_no_ext).lower()


def analyze_distribution(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    raw = profile_data.get("distribution", []) or []
    items = []
    for dist in raw:
        cr_type = str(dist.get("@type") or "").strip()
        kind = "file"
        if "DatabaseConnection" in cr_type:
            kind = "database"
        elif "FileSet" in cr_type:
            kind = "folder"
        items.append({
            "id": dist.get("@id"),
            "name": dist.get("name"),
            "kind": kind,
            "format": dist.get("encodingFormat"),
            "url": dist.get("contentUrl")
        })
    return {"total": len(items), "items": items}


def infer_content_type(profile_data: Dict[str, Any]) -> str:
    summary = analyze_distribution(profile_data)
    if any(i["kind"] == "database" for i in summary["items"]): return "SQL"
    formats = {str(i["format"]).lower() for i in summary["items"] if i["format"]}
    if any("pdf" in f for f in formats): return "MIXED/TEXTUAL"
    if any("csv" in f or "sql" in f or "excel" in f for f in formats): return "TABULAR"
    return "UNKNOWN"


# --------------------------------------------------------------------
# 2) Data extraction
# --------------------------------------------------------------------
def extract_txt_documents(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    found_docs = []
    for rs in profile_data.get("recordSet", []) or []:
        rs_name = rs.get("name", "")
        if str(rs_name).strip().lower().endswith((".pdf", ".txt", ".doc")):
            found_docs.append(str(rs_name).strip().lower())
    return {
        "all_document_names": sorted(list(set(found_docs))),
        "global_keywords": profile_data.get("keywords", [])
    }


def extract_csv_tables_with_samples(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    # Meta-mapping usando la funzione locale normalize_name
    dist_meta = {normalize_name(d.get("name")): d for d in profile_data.get("distribution", [])}
    tables = []
    all_columns_map = {}

    for rs in profile_data.get("recordSet", []) or []:
        rs_name = rs.get("name", "table")
        if rs_name.lower().endswith(".pdf"): continue

        meta = dist_meta.get(normalize_name(rs_name), {})
        cols = {}
        for field in rs.get("field", []) or []:
            orig = field.get("name")
            if orig:
                norm = normalize_name(orig)
                samples = set(str(s).strip().lower() for s in (field.get("sample") or []))
                cols[norm] = {"name": orig, "samples": samples}
                all_columns_map[norm] = orig

        if rs.get("examples"):
            try:
                ex = json.loads(rs["examples"])
                row = ex[0] if isinstance(ex, list) and len(ex) > 0 else (ex if isinstance(ex, dict) else {})
                for k, v in row.items():
                    norm = normalize_name(k)
                    if norm not in cols:
                        cols[norm] = {"name": k, "samples": set()}
                        all_columns_map[norm] = k
                    if isinstance(ex, dict) and isinstance(ex[k], list):
                        cols[norm]["samples"].update(str(x).strip().lower() for x in ex[k])
                    elif v:
                        cols[norm]["samples"].add(str(v).strip().lower())
            except:
                pass

        if cols:
            tables.append({
                "id": rs.get("@id", str(uuid.uuid4())),
                "name": rs_name,
                "columns": cols,
                "format": meta.get("encodingFormat", "text/csv"),
                "url": meta.get("contentUrl", "")
            })
    return {"tables": tables, "all_columns_map": all_columns_map,
            "all_normalized_names": sorted(list(all_columns_map.keys()))}


# --------------------------------------------------------------------
# 3) Compare functions
# --------------------------------------------------------------------
def compare_txt_files(txt1: Dict[str, Any], txt2: Dict[str, Any]) -> Dict[str, Any]:
    n1, n2 = set(txt1["all_document_names"]), set(txt2["all_document_names"])
    common_names = sorted(list(n1 & n2))
    kw1, kw2 = set(txt1["global_keywords"]), set(txt2["global_keywords"])
    return {
        "common_document_names": common_names,
        "common_document_keywords": sorted(list(kw1 & kw2)),
        "count_match": len(common_names)
    }


def compare_csv_schemas_with_samples(csv1: Dict[str, Any], csv2: Dict[str, Any]) -> Dict[str, Any]:
    norm1, norm2 = set(csv1["all_normalized_names"]), set(csv2["all_normalized_names"])
    common_norms = sorted(list(norm1 & norm2))
    per_column_overlap = []
    for norm in common_norms:
        s1, s2 = set(), set()
        for t in csv1["tables"]:
            if norm in t["columns"]: s1.update(t["columns"][norm]["samples"])
        for t in csv2["tables"]:
            if norm in t["columns"]: s2.update(t["columns"][norm]["samples"])
        common_data = sorted(list(s1 & s2))
        per_column_overlap.append({
            "column_detected": csv1["all_columns_map"][norm],
            "common_samples": common_data,
            "match_strength": "HIGH" if common_data else "SCHEMA_ONLY"
        })
    return {"common_columns": [csv1["all_columns_map"][n] for n in common_norms],
            "per_column_sample_overlap": per_column_overlap}


# --------------------------------------------------------------------
# 4) KG Generator
# --------------------------------------------------------------------
def build_graph_json(report: Dict, dp1: Dict, dp2: Dict) -> Dict:
    nodes = []
    edges = []
    root_id = str(uuid.uuid4())
    generated_at = datetime.now(timezone.utc).isoformat()

    def format_score(value, default=0.0):
        try:
            return round(float(value), 2)
        except (TypeError, ValueError):
            return default

    nodes.append({
        "id": root_id, "labels": ["BasicDLElement"],
        "properties": {
            "date": generated_at,
            "similarityScore": format_score(report.get("combined_similarity", 0)),
            "usesMetrics": "avg"
        }
    })

    ds1_id = report.get("dataprofile1ref", str(uuid.uuid4()))
    ds2_id = report.get("dataprofile2ref", str(uuid.uuid4()))

    # --- Chunk analysis (Explainability) ---
    similarity._ensure_models()
    desc1 = dp1.get("description", "")
    desc2 = dp2.get("description", "")
    chunks1 = split_chunks(desc1)
    chunks2 = split_chunks(desc2)
    desc_evidence = []

    if chunks1 and chunks2 and similarity._model_long is not None:
        emb1 = similarity._model_long.encode(chunks1, convert_to_tensor=True)
        emb2 = similarity._model_long.encode(chunks2, convert_to_tensor=True)
        sim_matrix = util.cos_sim(emb1, emb2)

        matches = []
        for idx, chunk in enumerate(chunks1):
            best_idx = torch.argmax(sim_matrix[idx]).item()
            score = sim_matrix[idx][best_idx].item()
            matches.append({'c1': chunk, 'c2': chunks2[best_idx], 'score': score})

        top_3 = sorted(matches, key=lambda x: x['score'], reverse=True)[:3]
        for i, m in enumerate(top_3, 1):
            desc_evidence.append({
                "rank": i,
                "similarity": f"{m['score']:.2%}",
                "chunk1": m['c1'],
                "chunk2": m['c2']
            })

    # --- KEYWORDS LOGIC (Case-Insensitive) ---
    raw_kw1 = dp1.get("keywords", []) or []
    raw_kw2 = dp2.get("keywords", []) or []
    norm_kw1 = normalize_keywords(raw_kw1)
    norm_kw2 = normalize_keywords(raw_kw2)
    common_set_lower = norm_kw1.intersection(norm_kw2)

    common_kw_display = []
    seen_lower = set()
    for k in (list(raw_kw1) + list(raw_kw2)):
        if not isinstance(k, str):
            continue
        kl = k.lower().strip()
        if kl in common_set_lower and kl not in seen_lower:
            common_kw_display.append(k)
            seen_lower.add(kl)

    # 2. Dataset nodes
    for ds_id, profile in [(ds1_id, dp1), (ds2_id, dp2)]:
        nodes.append({
            "id": ds_id, "labels": ["sc:Dataset"],
            "properties": {
                "type": "sc:Dataset", "name": profile.get("name"),
                "dg:description": profile.get("description", ""),
                "dg:headline": profile.get("headline", ""),
                "dg:keywords": list(profile.get("keywords", []))
            }
        })
        edges.append({"from": root_id, "to": ds_id, "labels": ["HAS_TARGET"]})

    # 3. Property comparison nodes
    metrics = [
        ("keywords", report.get("kw_sim", 0), report.get("kw_w", 0.6), "JaccardSimilarity", common_kw_display),
        ("descriptions", report.get("desc_sim", 0), report.get("desc_w", 0.3), "all-mpnet-base-v2", desc_evidence),
        ("headlines", report.get("headline_similarity", report.get("head_sim", 0)), report.get("head_w", 0.1),
         "all-mpnet-base-v2", None)
    ]

    description_comp_id = None

    for prop, val, weight_val, metric_name, evidence in metrics:
        comp_id = str(uuid.uuid4())
        comp_properties = {
            "targetProperty": prop,
            "similarityScore": format_score(val),
            "usesMetrics": metric_name
        }
        if prop == "keywords":
            comp_properties["relevantWords"] = evidence

        nodes.append({
            "id": comp_id, "labels": ["PropertyComparison"],
            "properties": comp_properties
        })
        edges.append({
            "from": root_id, "to": comp_id,
            "labels": ["HAS_COMPARISON"],
            "properties": {"weight": format_score(weight_val)}
        })

        edges.append({"from": comp_id, "to": ds1_id, "labels": ["HAS_TARGET"]})
        edges.append({"from": comp_id, "to": ds2_id, "labels": ["HAS_TARGET"]})

        if prop == "descriptions":
            description_comp_id = comp_id

    if description_comp_id:
        for item in desc_evidence:
            evidence_id = str(uuid.uuid4())
            nodes.append({
                "id": evidence_id,
                "labels": ["TextEvidence"],
                "properties": {"similarityScore": format_score(float(str(item.get("similarity", "0")).rstrip("%")))}
            })
            edges.append({
                "from": description_comp_id,
                "to": evidence_id,
                "labels": ["HAS_EVIDENCE"],
                "properties": {"rank": int(item.get("rank", 0))}
            })
            edges.append({
                "from": evidence_id,
                "to": ds1_id,
                "labels": ["HAS_TARGET"],
                "properties": {"chunk": item.get("chunk1", "")}
            })
            edges.append({
                "from": evidence_id,
                "to": ds2_id,
                "labels": ["HAS_TARGET"],
                "properties": {"chunk": item.get("chunk2", "")}
            })

    # 4. File matching
    dist1_map = {normalize_name(d.get("name")): d for d in dp1.get("distribution", []) if d.get("name")}
    dist2_map = {normalize_name(d.get("name")): d for d in dp2.get("distribution", []) if d.get("name")}
    linked_ids = set()
    common_documents = report.get("txt_comparison", {}).get("common_document_names", [])
    file_link_id = None

    def ensure_file_link() -> str:
        nonlocal file_link_id
        if file_link_id:
            return file_link_id
        file_link_id = str(uuid.uuid4())
        nodes.append({
            "id": file_link_id,
            "labels": ["FileObjectLinkingElement"],
            "properties": {
                "date": generated_at,
                "similarityScore": 100.0,
                "usesMetrics": "avg"
            }
        })
        edges.append({"from": root_id, "to": file_link_id, "labels": ["HAS_DETAIL"]})
        return file_link_id

    for fname in common_documents:
        fn = normalize_name(fname)
        m1, m2 = dist1_map.get(fn, {}), dist2_map.get(fn, {})
        f1_id, f2_id = m1.get("@id", str(uuid.uuid4())), m2.get("@id", str(uuid.uuid4()))

        for fid, meta, parent in [(f1_id, m1, ds1_id), (f2_id, m2, ds2_id)]:
            if fid not in linked_ids:
                nodes.append({
                    "id": fid, "labels": ["cr:FileObject"],
                    "properties": {
                        "name": meta.get("name", fname),
                        "type": "cr:FileObject",
                        "sc:encodingFormat": meta.get("encodingFormat", "application/pdf"),
                        "sc:contentUrl": meta.get("contentUrl", "")
                    }
                })
                edges.append({"from": parent, "to": fid, "labels": ["distribution"]})
                linked_ids.add(fid)

        link_id = ensure_file_link()
        file_comp_id = str(uuid.uuid4())
        nodes.append({
            "id": file_comp_id,
            "labels": ["FileObjectComparison"],
            "properties": {
                "targetProperty": "filename",
                "similarityScore": 100.0
            }
        })
        edges.append({
            "from": link_id,
            "to": file_comp_id,
            "labels": ["HAS_COMPARISON"],
            "properties": {"weight": 1.0}
        })
        edges.append({"from": file_comp_id, "to": f1_id, "labels": ["HAS_TARGET"]})
        edges.append({"from": file_comp_id, "to": f2_id, "labels": ["HAS_TARGET"]})

    # 5. CSV/table matching: only file objects with the same normalized name are
    # represented here. Sample overlaps are too noisy for FO-level evidence.
    csv_ext1, csv_ext2 = extract_csv_tables_with_samples(dp1), extract_csv_tables_with_samples(dp2)
    tables1 = {normalize_name(t["name"]): t for t in csv_ext1["tables"] if t.get("name")}
    tables2 = {normalize_name(t["name"]): t for t in csv_ext2["tables"] if t.get("name")}
    for table_key in sorted(set(tables1) & set(tables2)):
        t1, t2 = tables1[table_key], tables2[table_key]
        for td, parent in [(t1, ds1_id), (t2, ds2_id)]:
            if td["id"] not in linked_ids:
                nodes.append({
                    "id": td["id"], "labels": ["cr:FileObject"],
                    "properties": {
                        "name": td["name"],
                        "type": "cr:FileObject",
                        "sc:encodingFormat": td["format"],
                        "sc:contentUrl": td.get("url", "")
                    }
                })
                edges.append({"from": parent, "to": td["id"], "labels": ["distribution"]})
                linked_ids.add(td["id"])

        link_id = ensure_file_link()
        file_comp_id = str(uuid.uuid4())
        nodes.append({
            "id": file_comp_id,
            "labels": ["FileObjectComparison"],
            "properties": {
                "targetProperty": "filename",
                "similarityScore": 100.0
            }
        })
        edges.append({
            "from": link_id,
            "to": file_comp_id,
            "labels": ["HAS_COMPARISON"],
            "properties": {"weight": 1.0}
        })
        edges.append({"from": file_comp_id, "to": t1["id"], "labels": ["HAS_TARGET"]})
        edges.append({"from": file_comp_id, "to": t2["id"], "labels": ["HAS_TARGET"]})

    return {"nodes": nodes, "edges": edges}


# --------------------------------------------------------------------
# 5) Main Entrypoints
# --------------------------------------------------------------------
def refine_similarity(folder_path: Optional[str], dataprofile1: str, dataprofile2: str,
                      kw_s=0, desc_s=0, head_s=0, comb_s=0, kw_w=0.6, desc_w=0.3, head_w=0.1) -> Dict[str, Any]:
    """
    Versione Ibrida:
    - Se folder_path esiste, legge i file .json locali.
    - Se folder_path è None/vuoto, scarica i profili tramite API usando gli ID.
    """

    # --- 1. ACQUISIZIONE DATI ---
    if not folder_path:
        # MODALITÀ API: scarichiamo i profili Croissant in tempo reale
        from dl.similarity import get_access_token, DETAIL_URL, fix_encoding
        import requests

        print(f"☁️ Refining in API mode for IDs: {dataprofile1}, {dataprofile2}")
        token = get_access_token()
        headers = {'Authorization': f'Bearer {token}', 'accept': 'application/json'}

        try:
            # Recupero Profilo 1
            res1 = requests.get(f"{DETAIL_URL}{dataprofile1}?format=croissant", headers=headers, verify=False)
            res1.raise_for_status()
            dp1_raw = fix_encoding(res1.json()).get('dataset')

            # Recupero Profilo 2
            res2 = requests.get(f"{DETAIL_URL}{dataprofile2}?format=croissant", headers=headers, verify=False)
            res2.raise_for_status()
            dp2_raw = fix_encoding(res2.json()).get('dataset')

        except Exception as e:
            return {"error": f"API Retrieval Failed: {str(e)}"}
    else:
        # MODALITÀ LOCALE: logica basata su file system
        folder = Path(folder_path)
        dp1_raw = json.loads((folder / dataprofile1).read_text(encoding="utf-8"))
        dp2_raw = json.loads((folder / dataprofile2).read_text(encoding="utf-8"))

    if not dp1_raw or not dp2_raw:
        return {"error": "Failed to load one or both data profiles."}

    # --- 2. ANALISI GRANULARE ---
    txt1, txt2 = extract_txt_documents(dp1_raw), extract_txt_documents(dp2_raw)
    csv1, csv2 = extract_csv_tables_with_samples(dp1_raw), extract_csv_tables_with_samples(dp2_raw)

    txt_cmp = compare_txt_files(txt1, txt2)
    csv_cmp = compare_csv_schemas_with_samples(csv1, csv2)

    # --- 3. COSTRUZIONE REPORT ---
    report = {
        "dataprofile1ref": dp1_raw.get("@id"),
        "dataprofile2ref": dp2_raw.get("@id"),
        "content_type1": infer_content_type(dp1_raw),
        "content_type2": infer_content_type(dp2_raw),
        "txt_comparison": txt_cmp,
        "csv_comparison": csv_cmp,
        "kw_sim": kw_s, "desc_sim": desc_s, "head_sim": head_s, "combined_similarity": comb_s,
        "kw_w": kw_w, "desc_w": desc_w, "head_w": head_w
    }

    # --- 4. GENERAZIONE GRAFO (KG) ---
    graph = build_graph_json(report, dp1_raw, dp2_raw)
    report["graph"] = graph

    # Logging di debug nel terminale uvicorn
    sys.stderr.write("\n" + "=" * 60 + "\n### GRAPH JSON GENERATED (KG) ###\n")
    sys.stderr.write(json.dumps(graph, indent=2, ensure_ascii=False))
    sys.stderr.write("\n" + "=" * 60 + "\n")
    sys.stderr.flush()

    return report


def build_refinement_profile(report: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "@type": "RefinementReport",
        "generatedAtTime": datetime.now(timezone.utc).isoformat(),
        "graph": report.get("graph"),
        "results": {
            "matching_files": report["txt_comparison"]["common_document_names"],
            "matching_keywords": report["txt_comparison"]["common_document_keywords"],
            "matching_columns": report["csv_comparison"]["common_columns"],
            "data_overlaps": report["csv_comparison"]["per_column_sample_overlap"]
        }
    }
