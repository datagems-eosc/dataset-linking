# dl/flask_app.py
import io
import json
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote_plus

from flask import Flask, render_template, request, send_file, make_response

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


def _dataset_payload(data):
    if isinstance(data, dict) and isinstance(data.get("dataset"), dict):
        return data["dataset"]
    return data if isinstance(data, dict) else {}


def _load_profile(profile_id, folder_path, use_api, profile_cache):
    if profile_id in profile_cache:
        return profile_cache[profile_id]

    profile = None
    if use_api:
        token = get_access_token()
        headers = {'Authorization': f'Bearer {token}', 'accept': 'application/json'}
        response = requests.get(f"{DETAIL_URL}{profile_id}?format=croissant", headers=headers, timeout=25, verify=False)
        response.raise_for_status()
        profile = _dataset_payload(fix_encoding(response.json()))
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


def _attach_description_chunks(link, folder_path, use_api, profile_cache=None):
    if link.get("description_top_chunks"):
        return
    if profile_cache is not None:
        dp1 = _load_profile(link.get("dataprofile1ref"), folder_path, use_api, profile_cache)
        dp2 = _load_profile(link.get("dataprofile2ref"), folder_path, use_api, profile_cache)
        if dp1 and dp2:
            from dl import similarity

            similarity._ensure_models()
            link["description_top_chunks"] = similarity._top_description_chunks(
                dp1.get("description", ""),
                dp2.get("description", "")
            )
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


# ---------------------------------------------------------------------------- #
# Routes
# ---------------------------------------------------------------------------- #

@app.route("/")
def index():
    folder_path = get_requested_folder()

    # If there is no folder_path --> API
    use_api = True if not folder_path else False

    kw_weight, desc_weight, head_weight, th, normalized = get_weights_and_threshold()

    error, similarities, from_cache = compute_similarities(
        folder_path=folder_path if folder_path else None,
        kw_weight=kw_weight,
        desc_weight=desc_weight,
        head_weight=head_weight,
        threshold=th,
        use_api=use_api
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

        success_message += f" | threshold: {th:.0f}%"
        success = success_message

    return render_template(
        "index.html",
        similarities=similarities,
        folder=folder_path or "",
        kw=kw_weight,
        desc=desc_weight,
        head=head_weight,
        th=th,
        error=error,
        success=success
    )


@app.route("/save")
def save_results():
    folder_path = get_requested_folder()
    use_api = True if not folder_path else False

    kw_weight, desc_weight, head_weight, th, normalized = get_weights_and_threshold()

    error, similarities, _ = compute_similarities(
        folder_path=folder_path if folder_path else None,
        kw_weight=kw_weight,
        desc_weight=desc_weight,
        head_weight=head_weight,
        threshold=th,
        use_api=use_api
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
        "headline": head_weight, "normalized": normalized
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
        use_api=use_api
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
        _attach_description_chunks(link, folder_path if folder_path else None, use_api, profile_cache)
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

    kw_weight, desc_weight, head_weight, th, normalized = get_weights_and_threshold()
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

    error, similarities, from_cache = compute_similarities(
        folder_path=folder_path if folder_path else None,
        kw_weight=kw_weight,
        desc_weight=desc_weight,
        head_weight=head_weight,
        threshold=th,
        use_api=use_api
    )

    if error:
        return f"❌ Cannot build composite links: {error}", 400

    above_threshold = [s for s in similarities if s.get("passes_threshold")]
    weights = {
        "keywords": kw_weight,
        "description": desc_weight,
        "headline": head_weight,
        "normalized": normalized,
        "threshold": th,
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
        _attach_description_chunks(link, folder_path if folder_path else None, use_api, profile_cache)
        link["refinement_evidence"] = _build_refinement_evidence_for_link(
            link,
            folder_path if folder_path else None,
            use_api,
            profile_cache
        )

    composite_data = build_composite_datalinking_elements(
        base_report["links"],
        relation_threshold=relation_threshold,
        top_n=top_n_value,
    )
    selection = {
        "mode": "all_above_threshold",
        "minimum_combined_similarity": th,
        "minimum_relation_similarity": relation_threshold,
        "links_considered": len(base_report["links"]),
        "comparisons_used": len(composite_data["comparisons"]),
        "includes_description_chunks": True,
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
        "composites": composite_data["composites"],
        "comparisons": composite_data["comparisons"],
    }

    if output_format != "json":
        return render_template(
            "composite_links.html",
            report=output_data,
            levels=output_data["levels"],
            composites=output_data["composites"],
            download_url=_download_url_for_current_request(),
            folder=folder_path or "",
        )

    buffer = io.BytesIO()
    buffer.write(json.dumps(output_data, indent=4, ensure_ascii=False).encode("utf-8"))
    buffer.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"composite_links_{timestamp}.json"
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

    error, similarities, _ = compute_similarities(
        folder_path if folder_path else None,
        kw_weight=kw,
        desc_weight=desc,
        head_weight=head,
        threshold=th,
        use_api=use_api
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
            "threshold": th
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
