# dl/flask_app.py
import io
import json
import uuid
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, request, send_file, make_response

from dl.similarity import compute_similarities
from dl.reports import build_croissant_report
from dl.refine import refine_similarity, build_refinement_profile
from dl.utils import get_weights_and_threshold

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
    folder_path = request.args.get("folder", "").strip()
    folder_path = folder_path.replace("\\", "/").strip()

    return folder_path

# ---------------------------------------------------------------------------- #
# Routes
# ---------------------------------------------------------------------------- #
@app.route("/")
def index():
    folder_path = get_requested_folder()

    if not folder_path:
        return render_template(
            "index.html",
            similarities=[],
            folder="",
            error=None,
            success="📂 Please enter the path of a folder containing JSON profiles to start the analysis."
        )

    kw_weight, desc_weight, head_weight, th, normalized = get_weights_and_threshold()

    error, similarities, from_cache = compute_similarities(
        folder_path, kw_weight, desc_weight, head_weight, threshold=th
    )

    success = None
    if not error:
        folder_display = folder_path or (Path.home() / "Desktop" / "Profiles")
        success_message = (
            f"✅ Analysis completed successfully | Folder: {folder_display}"
            f" | weights → KW: {kw_weight:.2f}, DESC: {desc_weight:.2f}, HEAD: {head_weight:.2f}"
        )
        if normalized:
            success_message += " ⚖️ Weights were automatically normalized"
        if from_cache:
            success_message += " | ⚡ Loaded from cache."

        success_message += f" | threshold: {th:.0f}%"
        success = success_message

    return render_template(
        "index.html",
        similarities=similarities,
        folder=folder_path or "",
        error=error,
        success=success
    )


@app.route("/save")
def save_results():
    folder_path = get_requested_folder()

    kw_weight, desc_weight, head_weight, th, normalized = get_weights_and_threshold()

    error, similarities, _ = compute_similarities(
        folder_path, kw_weight, desc_weight, head_weight, threshold=th
    )
    if error:
        return f"❌ Cannot save results: {error}", 400

    folder_display = folder_path or (Path.home() / "Desktop" / "Profiles")
    weights = {
        "keywords": kw_weight,
        "description": desc_weight,
        "headline": head_weight,
        "normalized": normalized
    }

    # --- Load metadata for each JSON file ---
    file_data = {}
    for file in Path(folder_display).glob("*.json"):
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
                file_data[file.name] = {
                    "keywords": sorted(set(data.get("keywords", []))),
                    "description": data.get("description", ""),
                    "headline": data.get("headline", "")
                }
        except Exception as e:
            print(f"⚠️ Failed to load metadata from {file.name}: {e}")
            continue

    output_data = build_croissant_report(folder_display, weights, similarities, file_data)

    buffer = io.BytesIO()
    json_bytes = json.dumps(output_data, ensure_ascii=False, indent=4).encode("utf-8")
    buffer.write(json_bytes)
    buffer.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"similarity_{timestamp}.json"

    response = make_response(send_file(
        buffer,
        mimetype="application/json",
        as_attachment=True,
        download_name=filename
    ))
    response.set_cookie("downloadComplete", "1", max_age=10)
    return response


@app.route("/save_single")
def save_single():
    dataprofile1 = request.args.get("d1")
    dataprofile2 = request.args.get("d2")
    folder_path = request.args.get("folder")
    _, _, _, th, _ = get_weights_and_threshold()

    error, similarities, _ = compute_similarities(folder_path, threshold=th)
    if error:
        return f"❌ Cannot save results: {error}", 400

    match = next(
        (
            s for s in similarities
            if (s["dataprofile1"] == dataprofile1 and s["dataprofile2"] == dataprofile2)
               or (s["dataprofile1"] == dataprofile2 and s["dataprofile2"] == dataprofile1)
        ),
        None
    )
    if not match:
        return f"❌ Pair {dataprofile1} / {dataprofile2} not found.", 404

    folder_display = folder_path or (Path.home() / "Desktop" / "Profiles")

    output_data = {
        "@context": "http://mlcommons.org/croissant/",
        "@type": "DatasetSimilarityReport",
        "analyzedFolder": str(folder_display),
        "elements": [],
        "links": []
    }

    for dp in [dataprofile1, dataprofile2]:
        element = {
            "@type": "DLElement",
            "@id": f"profile:{dp.replace('.json', '')}",
            "name": dp,
            "description": "",
            "keywords": [],
            "headline": "",
            "source": {
                "@type": "DataDownload",
                "contentUrl": f"file:///{folder_display}/{dp}",
                "encodingFormat": "application/json"
            }
        }
        output_data["elements"].append(element)

    link = {
        "@type": "SimilarityLink",
        "@id": f"link:{uuid.uuid4()}",
        "dataprofile1": f"profile:{match['dataprofile1'].replace('.json', '')}",
        "dataprofile2": f"profile:{match['dataprofile2'].replace('.json', '')}",
        "dataprofile1id": match.get("id1"),
        "dataprofile2id": match.get("id2"),
        "metrics": {
            "keywords_similarity": match["keywords_similarity"],
            "description_similarity": match["description_similarity"],
            "headline_similarity": match["headline_similarity"],
            "combined_similarity": match["combined_similarity"]
        },
        "common_keywords": [
            kw.strip()
            for kw in match["common_keywords"].split(",")
            if kw.strip()
        ]
    }

    output_data["links"].append(link)

    buffer = io.BytesIO()
    json_bytes = json.dumps(output_data, ensure_ascii=False, indent=4).encode("utf-8")
    buffer.write(json_bytes)
    buffer.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"similarity_{dataprofile1.replace('.json','')}_{dataprofile2.replace('.json','')}_{timestamp}.json"

    response = make_response(send_file(
        buffer,
        mimetype="application/json",
        as_attachment=True,
        download_name=filename
    ))
    response.set_cookie("downloadComplete", "1", max_age=10)
    return response

@app.route("/refine")
def refine_pair():
    import json as _json

    dataprofile1 = request.args.get("d1", "")
    dataprofile2 = request.args.get("d2", "")
    folder_path = request.args.get("folder", "") or ""
    # Always normalize: trim spaces and convert backslashes to slashes
    folder_path = folder_path.strip().replace("\\", "/")

    kw_weight, desc_weight, head_weight, threshold, _ = get_weights_and_threshold()

    # Diagnostic log (printed in the Flask console)
    print(f"[REFINE] d1={dataprofile1} d2={dataprofile2} folder={folder_path}")

    # Quick validation: if something is missing, fail fast with a visible error
    if not (dataprofile1 and dataprofile2 and folder_path):
        return make_response(
            f"<pre>❌ Missing params.\n"
            f"d1={dataprofile1}\n"
            f"d2={dataprofile2}\n"
            f"folder={folder_path}\n</pre>", 400
        )

    try:
        result = refine_similarity(
            folder_path,
            dataprofile1,
            dataprofile2,
            kw_weight,
            desc_weight,
            head_weight,
            threshold
        )
    except Exception as e:
        # If something goes wrong, respond immediately (avoid “infinite loading”)
        return make_response(f"<pre>❌ Exception in refine_similarity: {e!r}</pre>", 500)

    if isinstance(result, dict) and "error" in result:
        return make_response(f"<pre>❌ {result['error']}</pre>", 400)

    html = f"""
        <h2>🔁 Refinement Report</h2>
        <p><b>Profiles:</b> {dataprofile1} / {dataprofile2}</p>

        <p>
          <a href="/refine_download?d1={dataprofile1}&d2={dataprofile2}&folder={folder_path}"
             style="font-size:1em; padding:0.3em 0.6em; border:1px solid #ccc; border-radius:4px; text-decoration:none;">
            ⬇️ Download refinement JSON
          </a>
        </p>

        <pre>{_json.dumps(result, indent=4, ensure_ascii=False)}</pre>
        <p><a href="/" style="font-size:1.2em;">⬅️ Back to main page</a></p>
        """
    return html

@app.route("/refine_download")
def refine_download():
    dataprofile1 = request.args.get("d1", "")
    dataprofile2 = request.args.get("d2", "")
    folder_path = request.args.get("folder", "") or ""
    folder_path = folder_path.strip().replace("\\", "/")

    if not (dataprofile1 and dataprofile2 and folder_path):
        return make_response(
            f"<pre>❌ Missing params for download.\n"
            f"d1={dataprofile1}\n"
            f"d2={dataprofile2}\n"
            f"folder={folder_path}\n</pre>", 400
        )

    try:
        report = refine_similarity(
            folder_path,
            dataprofile1,
            dataprofile2,
        )
    except Exception as e:
        return make_response(f"<pre>❌ Exception in refine_similarity: {e!r}</pre>", 500)

    if isinstance(report, dict) and "error" in report:
        return make_response(f"<pre>❌ {report['error']}</pre>", 400)

    profile = build_refinement_profile(report)

    buffer = io.BytesIO()
    json_bytes = json.dumps(profile, ensure_ascii=False, indent=4).encode("utf-8")
    buffer.write(json_bytes)
    buffer.seek(0)

    filename = f"{dataprofile1.replace('.json','')}__{dataprofile2.replace('.json','')}.refinement.json"

    response = make_response(send_file(
        buffer,
        mimetype="application/json",
        as_attachment=True,
        download_name=filename
    ))
    # download complete
    response.set_cookie("downloadComplete", "1", max_age=10)
    return response
