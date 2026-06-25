import hashlib
import json
import requests
import sys
import io
from pathlib import Path
from itertools import combinations
from typing import Optional, Tuple, List, Dict, Any
from sentence_transformers import SentenceTransformer, util
from dl.utils import normalize_keywords

# --- GLOBALS ---
_model_short: Optional[SentenceTransformer] = None
_model_long: Optional[SentenceTransformer] = None

# --- TERMINAL ENCODING FIX ---
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# --- CONFIGURATION ---
BASE_URL = "https://datagems-dev.scayle.es"
AUTH_URL = f"{BASE_URL}/oauth/realms/dev/protocol/openid-connect/token"
SEARCH_URL = f"{BASE_URL}/dmm/api/v1/dataset/search"
DETAIL_URL = f"{BASE_URL}/dmm/api/v1/dataset/get/"


# --- UTILS & MODELS ---

def _ensure_models():
    """Load SentenceTransformer models only if necessary (Correct global usage)"""
    global _model_short, _model_long
    if _model_short is None or _model_long is None:
        print("Wait... Loading AI models...")
        # Lazy import inside to avoid overhead if not needed
        from sentence_transformers import SentenceTransformer
        _model_short = SentenceTransformer("all-MiniLM-L6-v2")
        _model_long = SentenceTransformer("all-mpnet-base-v2")


def get_iteration_fingerprint(dataset_ids: List[str], weights: Tuple[float, float, float]) -> str:
    """Creates a unique hash based on IDs and weights."""
    dataset_ids.sort()
    ids_string = ",".join(dataset_ids)
    weights_string = f"{weights[0]:.2f}-{weights[1]:.2f}-{weights[2]:.2f}"
    full_string = f"{ids_string}|{weights_string}"
    return hashlib.md5(full_string.encode()).hexdigest()


def fix_encoding(data: Any) -> Any:
    """Recursively fixes encoding issues (Mojibake latin-1 to utf-8)"""
    if isinstance(data, str):
        try:
            return data.encode('latin-1').decode('utf-8')
        except:
            return data
    elif isinstance(data, list):
        return [fix_encoding(i) for i in data]
    elif isinstance(data, dict):
        return {k: fix_encoding(v) for k, v in data.items()}
    return data


# --- API FUNCTIONS ---

def get_access_token() -> str:
    payload = {
        'grant_type': 'password', 'client_id': 'swagger-client',
        'username': 'dg-user-admin', 'password': 'dg-user-admin', 'scope': 'datagems'
    }
    response = requests.post(AUTH_URL, data=payload, timeout=10, verify=False)
    response.raise_for_status()
    return response.json().get('access_token')


def fetch_dataset_list(token: str) -> List[Dict[str, Any]]:
    """Fast call to get only the IDs and names (Discovery)"""
    headers = {'Authorization': f'Bearer {token}', 'accept': 'application/json'}
    resp = requests.get(f"{SEARCH_URL}?properties=name", headers=headers, timeout=30, verify=False)
    resp.raise_for_status()
    return fix_encoding(resp.json()).get('datasets', [])


def fetch_details_from_list(token: str, datasets_raw: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    headers = {'Authorization': f'Bearer {token}', 'accept': 'application/json'}
    file_data = {}

    for item in datasets_raw:
        try:
            nodes = item.get('nodes', [])
            if not nodes: continue

            ds_id = nodes[0].get('id')
            ds_name = nodes[0].get('properties', {}).get('name', f"ds_{ds_id}")

            print(f"📥 Fetching details for: {ds_name} (ID: {ds_id})...")
            res = requests.get(f"{DETAIL_URL}{ds_id}?format=croissant", headers=headers, timeout=25, verify=False)
            res.raise_for_status()

            inner_dataset = fix_encoding(res.json()).get('dataset', {})
            if not inner_dataset: continue

            file_data[ds_id] = {
                "name": ds_name,
                "status": str(inner_dataset.get('status', 'unknown')),  # Mantiene traccia dello status
                "keywords": normalize_keywords(inner_dataset.get('keywords', [])) if isinstance(
                    inner_dataset.get('keywords'), list) else set(),
                "description": inner_dataset.get('description', 'N/A'),
                "headline": inner_dataset.get('name', 'N/A'),
                "id": inner_dataset.get("@id", ds_id)
            }

            # Se lo status è 'ready', aggiungiamo il dataset,
            # anche se il nome esiste già (ds_id come chiave garantisce l'unicità)
            file_data[ds_id] = {
                "name": ds_name,
                "keywords": normalize_keywords(inner_dataset.get('keywords', [])) if isinstance(
                    inner_dataset.get('keywords'), list) else set(),
                "description": inner_dataset.get('description', 'N/A'),
                "headline": inner_dataset.get('name', 'N/A'),
                "id": inner_dataset.get("@id", ds_id)
            }
        except Exception as e:
            print(f"⚠️ Error in dataset {ds_id}: {e}")
            continue
    return file_data


def fetch_profiles_from_api() -> Dict[str, Dict[str, Any]]:
    """Compatibility wrapper for flask_app.py imports"""
    token = get_access_token()
    datasets_raw = fetch_dataset_list(token)
    return fetch_details_from_list(token, datasets_raw)


# --- CORE LOGIC ---

def compute_similarities(
        folder_path: Optional[str],
        kw_weight: float = 0.6,
        desc_weight: float = 0.3,
        head_weight: float = 0.1,
        threshold: float = 30.0,
        use_api: bool = True
) -> Tuple[Optional[str], List[Dict[str, Any]], Optional[bool]]:
    weights = (kw_weight, desc_weight, head_weight)
    folder = Path(folder_path) if folder_path else Path("/s3/cache")

    current_ids = []
    datasets_raw = []
    token = None

    # --- 1. Discovery Phase (IDs only) ---
    if use_api:
        try:
            token = get_access_token()
            datasets_raw = fetch_dataset_list(token)
            for item in datasets_raw:
                nodes = item.get('nodes', [])
                if nodes: current_ids.append(nodes[0].get('id'))
        except Exception as e:
            return f"❌ API Connection Error: {e}", [], False
    else:
        if not folder.exists(): return f"❌ Folder not found", [], False
        json_files = list(folder.glob("*.json"))
        current_ids = [f.name for f in json_files]

    # --- 2. Smart Cache Check with Fingerprint ---
    fingerprint = get_iteration_fingerprint(current_ids, weights)
    source_type = "api" if use_api else "local"
    cache_path = folder / f"cache_{source_type}_{fingerprint}.json"

    if cache_path.exists():
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                similarities = json.load(f)

            # Apply threshold dynamically on cached data
            for s in similarities:
                s["passes_threshold"] = s.get("combined_similarity", 0) >= threshold

            print(f"🟢 Smart Cache Hit! (Fingerprint: {fingerprint})")
            return None, similarities, True
        except Exception as e:
            print(f"⚠️ Cache read error, recomputing: {e}")

    # --- 3. Cache Miss: Full Data Acquisition ---
    print("🔄 Cache miss or data changed. Starting full analysis...")
    file_data = {}

    if use_api:
        file_data = fetch_details_from_list(token, datasets_raw)
    else:
        # LOGICA PER FILE LOCALI (Tipo: Era5land_3166e649...)
        for file in folder.glob("*.json"):
            try:
                with open(file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Usiamo l'ID interno se esiste, altrimenti l'INTERO nome del file
                # In questo modo id1 sarà "Era5land_3166e649-54c1-4ebf-904e-de9a46cb1b18.json"
                ds_id = data.get("@id", file.name)

                file_data[ds_id] = {
                    "name": data.get('name', file.stem.split('_')[0]),
                    # Prende "Era5land" dal nome file se manca nel JSON
                    "keywords": normalize_keywords(data.get('keywords', [])),
                    "description": data.get('description', ''),
                    "headline": data.get('headline', ''),
                    "id": ds_id
                }
            except Exception as e:
                print(f"⚠️ Errore file {file.name}: {e}")
                continue

    if not file_data or len(file_data) < 2:
        return "⚠️ Insufficient data for comparison.", [], False

    # --- 4. Similarity Computation ---
    _ensure_models()

    similarities = []
    for id1, id2 in combinations(file_data.keys(), 2):
        f1, f2 = file_data[id1], file_data[id2]

        kw1, kw2 = f1["keywords"], f2["keywords"]
        common = kw1 & kw2
        union = kw1 | kw2
        kw_sim = (len(common) / len(union) * 100) if union else 0

        emb_desc1 = _model_long.encode(f1["description"], convert_to_tensor=True)
        emb_desc2 = _model_long.encode(f2["description"], convert_to_tensor=True)
        emb_head1 = _model_short.encode(f1["headline"], convert_to_tensor=True)
        emb_head2 = _model_short.encode(f2["headline"], convert_to_tensor=True)

        desc_sim = max(0.0, min(1.0, util.cos_sim(emb_desc1, emb_desc2).item()))
        head_sim = max(0.0, min(1.0, util.cos_sim(emb_head1, emb_head2).item()))

        combined = (kw_weight * (kw_sim / 100) + desc_weight * desc_sim + head_weight * head_sim) * 100

        similarities.append({
            "dataprofile1": f"{f1['name']} ({id1[:5]})",
            "dataprofile2": f"{f2['name']} ({id2[:5]})",
            "id1": id1,
            "id2": id2,
            "keywords_similarity": round(kw_sim, 2),
            "description_similarity": round(desc_sim * 100, 2),
            "headline_similarity": round(head_sim * 100, 2),
            "combined_similarity": round(combined, 2),
            "common_keywords": ", ".join(sorted(common)),
            "common_count": len(common),
            "unique_to_1": ", ".join(sorted(kw1 - kw2)),
            "unique_to_2": ", ".join(sorted(kw2 - kw1)),
            "passes_threshold": combined >= threshold
        })

        print(f"✅ Processed pair: {id1[:5]} vs {id2[:5]}")

    # --- 5. Save Smart Cache ---
    try:
        folder.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(similarities, f, ensure_ascii=False, indent=4)
        print(f"💾 Smart Cache saved: {cache_path.name}")
    except Exception as e:
        print(f"⚠️ Cache save failed: {e}")

    return None, similarities, False