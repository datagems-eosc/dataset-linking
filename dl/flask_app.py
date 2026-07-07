# dl/flask_app.py
import io
import json
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote_plus

from flask import Flask, render_template, request, send_file, make_response

from dl.similarity import compute_similarities
from dl.reports import build_croissant_report
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
                "combined_similarity": match["combined_similarity"]
            },
            "common_keywords": [kw.strip() for kw in match["common_keywords"].split(",") if kw.strip()],
            "unique_to_1": [kw.strip() for kw in match.get("unique_to_1", "").split(",") if kw.strip()],
            "unique_to_2": [kw.strip() for kw in match.get("unique_to_2", "").split(",") if kw.strip()],
            "description_top_chunks": match.get("description_top_chunks", [])
        }]
    }

    buffer = io.BytesIO()
    # ensure_ascii=False per gestire correttamente i caratteri speciali
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
