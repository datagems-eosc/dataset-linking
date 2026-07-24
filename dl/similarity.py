import hashlib
import json
import pickle
import re
import requests
import sys
import io
from pathlib import Path
from itertools import combinations
from typing import Optional, Tuple, List, Dict, Any
from sentence_transformers import SentenceTransformer, util
import torch
from dl.utils import normalize_keywords, keywords_to_text

# --- GLOBALS ---
_model_short: Optional[SentenceTransformer] = None
_model_long: Optional[SentenceTransformer] = None

MODEL_SHORT_NAME = "all-MiniLM-L6-v2"
MODEL_LONG_NAME = "all-mpnet-base-v2"
EMBEDDING_CACHE_VERSION = "profile_embeddings_v1"

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
        _model_short = SentenceTransformer(MODEL_SHORT_NAME)
        _model_long = SentenceTransformer(MODEL_LONG_NAME)


def get_iteration_fingerprint(
        dataset_ids: List[str],
        weights: Tuple[float, float, float],
        source_signature: str = "",
        include_description_chunks: bool = False
) -> str:
    """Creates a unique hash based on IDs and weights."""
    dataset_ids.sort()
    ids_string = ",".join(dataset_ids)
    weights_string = f"{weights[0]:.2f}-{weights[1]:.2f}-{weights[2]:.2f}"
    chunk_mode = "all_description_chunks_v1" if include_description_chunks else "no_description_chunks_v1"
    full_string = f"{ids_string}|{weights_string}|{source_signature}|{chunk_mode}|dedupe_ready_v1|pairwise_effective_weights_v3|keyword_sbert_hierarchy_v1|headline_name_fallback_v1"
    return hashlib.md5(full_string.encode()).hexdigest()


def _json_fingerprint(data: Any) -> str:
    payload = json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _file_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.name.encode("utf-8"))
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _text_fingerprint(text: Any) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()


def _has_text(value: Any) -> bool:
    return bool(str(value or "").strip())


def _embedding_cache_path(folder: Path, source_type: str) -> Path:
    return folder / f"embedding_cache_{source_type}.pkl"


def _load_embedding_cache(cache_path: Path) -> Dict[str, Any]:
    if not cache_path.exists():
        return {"version": EMBEDDING_CACHE_VERSION, "entries": {}}

    try:
        with open(cache_path, "rb") as f:
            cache = pickle.load(f)
        if cache.get("version") != EMBEDDING_CACHE_VERSION or not isinstance(cache.get("entries"), dict):
            return {"version": EMBEDDING_CACHE_VERSION, "entries": {}}
        return cache
    except Exception as e:
        print(f"⚠️ Embedding cache read error, rebuilding: {e}")
        return {"version": EMBEDDING_CACHE_VERSION, "entries": {}}


def _save_embedding_cache(cache_path: Path, cache: Dict[str, Any]) -> None:
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "wb") as f:
            pickle.dump(cache, f)
        print(f"💾 Embedding cache saved: {cache_path.name}")
    except Exception as e:
        print(f"⚠️ Embedding cache save failed: {e}")


def _encode_texts_with_cache(
        profile_ids: List[str],
        texts: List[str],
        model: SentenceTransformer,
        model_name: str,
        field_name: str,
        cache: Dict[str, Any]
) -> Dict[str, torch.Tensor]:
    entries = cache.setdefault("entries", {})
    embeddings = {}
    missing_ids = []
    missing_texts = []

    for profile_id, text in zip(profile_ids, texts):
        text_hash = _text_fingerprint(text)
        cache_key = f"{field_name}:{model_name}:{profile_id}"
        cached = entries.get(cache_key)
        if cached and cached.get("text_hash") == text_hash:
            embeddings[profile_id] = torch.tensor(cached["embedding"])
            continue

        missing_ids.append(profile_id)
        missing_texts.append(text or "")

    if missing_texts:
        print(f"🔢 Encoding {len(missing_texts)} {field_name} embedding(s)...")
        encoded = model.encode(missing_texts, convert_to_tensor=True)
        for idx, profile_id in enumerate(missing_ids):
            embedding = encoded[idx].detach().cpu()
            text = missing_texts[idx]
            cache_key = f"{field_name}:{model_name}:{profile_id}"
            entries[cache_key] = {
                "text_hash": _text_fingerprint(text),
                "embedding": embedding.tolist()
            }
            embeddings[profile_id] = embedding

    return embeddings


def _dataset_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(data, dict) and isinstance(data.get("dataset"), dict):
        return data["dataset"]
    return data if isinstance(data, dict) else {}


def _is_ready_dataset(data: Dict[str, Any]) -> bool:
    status = str(data.get("status", "ready") or "ready").strip().lower()
    return status == "ready"


def _normalize_dataset_name(name: Any) -> str:
    return re.sub(r"\s+", " ", str(name or "").strip().lower())


def _profile_completeness_score(data: Dict[str, Any], keywords: set) -> int:
    distributions = data.get("distribution", []) or []
    record_sets = data.get("recordSet", []) or []
    if isinstance(distributions, dict):
        distributions = [distributions]
    if isinstance(record_sets, dict):
        record_sets = [record_sets]

    field_count = 0
    for record_set in record_sets:
        fields = record_set.get("field", []) if isinstance(record_set, dict) else []
        if isinstance(fields, dict):
            fields = [fields]
        field_count += len(fields or [])

    return (
        len(str(data.get("description", "") or ""))
        + len(str(data.get("headline", "") or ""))
        + len(str(data.get("name", "") or ""))
        + len(keywords) * 20
        + len(distributions) * 10
        + len(record_sets) * 20
        + field_count * 3
    )


def _dedupe_profiles_by_name(file_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    selected = {}
    skipped = 0

    for profile_id, profile in file_data.items():
        name_key = _normalize_dataset_name(profile.get("name")) or profile_id
        current = selected.get(name_key)
        if current is None or profile.get("__completeness_score", 0) > current[1].get("__completeness_score", 0):
            if current is not None:
                skipped += 1
            selected[name_key] = (profile_id, profile)
        else:
            skipped += 1

    if skipped:
        print(f"ℹ️ Skipped {skipped} duplicate dataset profile(s), keeping the most complete version per name.")

    deduped = {}
    for profile_id, profile in selected.values():
        profile.pop("__completeness_score", None)
        deduped[profile_id] = profile
    return deduped


def _split_chunks(text: str) -> List[str]:
    if not text or len(str(text)) < 20:
        return []

    clean_text = re.sub(r"\s+", " ", str(text)).strip()
    chunks = re.split(r"[,.:;]| and | or | but ", clean_text)
    clean_chunks = [
        chunk.strip()
        for chunk in chunks
        if len(chunk.strip().split()) >= 3 and len(chunk.strip()) > 20
    ]
    if not clean_chunks and len(clean_text.split()) >= 3:
        return [clean_text]
    return clean_chunks


def _top_description_chunks(text1: str, text2: str, limit: int = 3) -> List[Dict[str, Any]]:
    chunks1 = _split_chunks(text1)
    chunks2 = _split_chunks(text2)
    if not chunks1 or not chunks2 or _model_long is None:
        return []

    emb1 = _model_long.encode(chunks1, convert_to_tensor=True)
    emb2 = _model_long.encode(chunks2, convert_to_tensor=True)
    scores = util.cos_sim(emb1, emb2)

    matches = []
    for idx, chunk in enumerate(chunks1):
        best_idx = scores[idx].argmax().item()
        matches.append({
            "chunk1": chunk,
            "chunk2": chunks2[best_idx],
            "similarity": round(scores[idx][best_idx].item() * 100, 2)
        })

    top_matches = sorted(matches, key=lambda item: item["similarity"], reverse=True)[:limit]
    for rank, match in enumerate(top_matches, 1):
        match["rank"] = rank
    return top_matches


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
    params = {
        'properties': 'name',
        'count': 200,
        'dataset_status': 'ready'
    }
    resp = requests.get(SEARCH_URL, headers=headers, params=params, timeout=30, verify=False)
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

            res = requests.get(f"{DETAIL_URL}{ds_id}?format=croissant", headers=headers, timeout=25, verify=False)
            res.raise_for_status()

            inner_dataset = _dataset_payload(fix_encoding(res.json()))
            if not inner_dataset: continue
            if not _is_ready_dataset(inner_dataset): continue

            raw_keywords = inner_dataset.get('keywords', [])
            keywords = normalize_keywords(raw_keywords)
            headline = inner_dataset.get('headline') or inner_dataset.get('name') or ds_name or ''

            # COSTRUZIONE DEL DIZIONARIO (Assicurati di non sovrascrivere!)
            file_data[ds_id] = {
                "name": ds_name,
                "status": str(inner_dataset.get('status', 'ready')), # Default 'ready' se manca
                "keywords": keywords,
                "keyword_text": keywords_to_text(raw_keywords),
                "description": inner_dataset.get('description', ''),
                "headline": headline,
                "id": inner_dataset.get("@id", ds_id),
                "__completeness_score": _profile_completeness_score(inner_dataset, keywords)
            }
        except Exception as e:
            print(f"⚠️ Error in dataset {ds_id}: {e}")
            continue
    return _dedupe_profiles_by_name(file_data)


def fetch_profiles_from_api() -> Dict[str, Dict[str, Any]]:
    """Compatibility wrapper for flask_app.py imports"""
    token = get_access_token()
    datasets_raw = fetch_dataset_list(token)
    return fetch_details_from_list(token, datasets_raw)


def build_description_top_chunks_for_pair(
        folder_path: Optional[str],
        id1: str,
        id2: str,
        use_api: bool = True
) -> List[Dict[str, Any]]:
    _ensure_models()

    if use_api:
        token = get_access_token()
        headers = {'Authorization': f'Bearer {token}', 'accept': 'application/json'}
        res1 = requests.get(f"{DETAIL_URL}{id1}?format=croissant", headers=headers, timeout=25, verify=False)
        res2 = requests.get(f"{DETAIL_URL}{id2}?format=croissant", headers=headers, timeout=25, verify=False)
        res1.raise_for_status()
        res2.raise_for_status()
        dp1 = _dataset_payload(fix_encoding(res1.json()))
        dp2 = _dataset_payload(fix_encoding(res2.json()))
        return _top_description_chunks(dp1.get("description", ""), dp2.get("description", ""))

    folder = Path(folder_path) if folder_path else None
    if not folder or not folder.exists():
        return []

    dp1 = dp2 = None
    for file in folder.glob("*.json"):
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = _dataset_payload(json.load(f))
            current_id = data.get("@id", file.name)
            if current_id == id1:
                dp1 = data
            if current_id == id2:
                dp2 = data
        except Exception:
            continue

    if not dp1 or not dp2:
        return []
    return _top_description_chunks(dp1.get("description", ""), dp2.get("description", ""))


# --- CORE LOGIC ---

def compute_similarities(
        folder_path: Optional[str],
        kw_weight: float = 0.6,
        desc_weight: float = 0.3,
        head_weight: float = 0.1,
        threshold: float = 30.0,
        use_api: bool = True,
        include_description_chunks: bool = False
) -> Tuple[Optional[str], List[Dict[str, Any]], Optional[bool]]:
    weights = (kw_weight, desc_weight, head_weight)
    folder = Path(folder_path) if folder_path else Path("/s3/cache")
    source_type = "api" if use_api else "local"

    current_ids = []
    datasets_raw = []
    source_signature = ""
    token = None

    # --- 1. Discovery Phase (IDs only) ---
    if use_api:
        try:
            token = get_access_token()
            datasets_raw = fetch_dataset_list(token)
            source_signature = _json_fingerprint(datasets_raw)
            for item in datasets_raw:
                nodes = item.get('nodes', [])
                if nodes: current_ids.append(nodes[0].get('id'))
        except Exception as e:
            return f"❌ API Connection Error: {e}", [], False
    else:
        if not folder.exists(): return f"❌ Folder not found", [], False
        json_files = list(folder.glob("*.json"))
        current_ids = [f.name for f in json_files]
        source_signature = _json_fingerprint([
            _file_fingerprint(file)
            for file in sorted(json_files, key=lambda item: item.name)
        ])

    # --- 2. Smart Cache Check with Fingerprint ---
    fingerprint = get_iteration_fingerprint(current_ids, weights, source_signature, include_description_chunks)
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

                data = _dataset_payload(data)

                raw_keywords = data.get('keywords', [])
                keywords = normalize_keywords(raw_keywords)

                # Usiamo l'ID interno se esiste, altrimenti l'INTERO nome del file
                # In questo modo id1 sarà "Era5land_3166e649-54c1-4ebf-904e-de9a46cb1b18.json"
                ds_id = data.get("@id", file.name)

                name = data.get('name', file.stem.split('_')[0])
                file_data[ds_id] = {
                    "name": name,
                    # Prende "Era5land" dal nome file se manca nel JSON
                    "keywords": keywords,
                    "keyword_text": keywords_to_text(raw_keywords),
                    "description": data.get('description', ''),
                    "headline": data.get('headline') or name or '',
                    "id": ds_id,
                    "__completeness_score": _profile_completeness_score(data, keywords)
                }
            except Exception as e:
                print(f"⚠️ Errore file {file.name}: {e}")
                continue

        file_data = _dedupe_profiles_by_name(file_data)

    if not file_data or len(file_data) < 2:
        return "⚠️ Insufficient data for comparison.", [], False

    # --- 4. Similarity Computation ---
    _ensure_models()

    profile_ids = list(file_data.keys())
    embedding_cache_path = _embedding_cache_path(folder, source_type)
    embedding_cache = _load_embedding_cache(embedding_cache_path)
    keyword_embeddings = _encode_texts_with_cache(
        profile_ids,
        [file_data[profile_id].get("keyword_text", "") for profile_id in profile_ids],
        _model_short,
        MODEL_SHORT_NAME,
        "keywords_sbert",
        embedding_cache
    )
    desc_embeddings = _encode_texts_with_cache(
        profile_ids,
        [file_data[profile_id]["description"] for profile_id in profile_ids],
        _model_long,
        MODEL_LONG_NAME,
        "description",
        embedding_cache
    )
    head_embeddings = _encode_texts_with_cache(
        profile_ids,
        [file_data[profile_id]["headline"] for profile_id in profile_ids],
        _model_short,
        MODEL_SHORT_NAME,
        "headline",
        embedding_cache
    )
    _save_embedding_cache(embedding_cache_path, embedding_cache)

    similarities = []
    for id1, id2 in combinations(file_data.keys(), 2):
        f1, f2 = file_data[id1], file_data[id2]

        kw1, kw2 = f1["keywords"], f2["keywords"]
        common = kw1 & kw2
        union = kw1 | kw2
        keywords_used = bool(union)
        kw_sim = max(0.0, min(1.0, util.cos_sim(keyword_embeddings[id1], keyword_embeddings[id2]).item())) if keywords_used else 0.0

        desc_sim = max(0.0, min(1.0, util.cos_sim(desc_embeddings[id1], desc_embeddings[id2]).item()))
        head_sim = max(0.0, min(1.0, util.cos_sim(head_embeddings[id1], head_embeddings[id2]).item()))

        description_used = _has_text(f1.get("description")) and _has_text(f2.get("description"))
        headline_used = _has_text(f1.get("headline")) and _has_text(f2.get("headline"))

        active_weights = {
            "keywords": kw_weight if keywords_used else 0.0,
            "description": desc_weight if description_used else 0.0,
            "headline": head_weight if headline_used else 0.0,
        }
        active_total = sum(active_weights.values())
        if active_total > 0:
            effective_kw_weight = active_weights["keywords"] / active_total
            effective_desc_weight = active_weights["description"] / active_total
            effective_head_weight = active_weights["headline"] / active_total
        else:
            effective_kw_weight = 0.0
            effective_desc_weight = 0.0
            effective_head_weight = 0.0

        combined = (
            effective_kw_weight * kw_sim
            + effective_desc_weight * desc_sim
            + effective_head_weight * head_sim
        ) * 100
        description_top_chunks = []
        if include_description_chunks:
            description_top_chunks = _top_description_chunks(f1["description"], f2["description"])

        similarities.append({
            #"dataprofile1": f"{f1['name']} ({id1[:5]})",
            #"dataprofile2": f"{f2['name']} ({id2[:5]})",
            "dataprofile1": f"{f1['name']}",
            "dataprofile2": f"{f2['name']}",
            "id1": id1,
            "id2": id2,
            "keywords_similarity": round(kw_sim * 100, 2),
            "description_similarity": round(desc_sim * 100, 2),
            "headline_similarity": round(head_sim * 100, 2),
            "combined_similarity": round(combined, 2),
            "headline_used_in_score": headline_used,
            "field_usage": {
                "keywords": keywords_used,
                "description": description_used,
                "headline": headline_used,
            },
            "effective_weights": {
                "keywords": round(effective_kw_weight, 4),
                "description": round(effective_desc_weight, 4),
                "headline": round(effective_head_weight, 4),
            },
            "common_keywords": ", ".join(sorted(common)),
            "common_count": len(common),
            "unique_to_1": ", ".join(sorted(kw1 - kw2)),
            "unique_to_2": ", ".join(sorted(kw2 - kw1)),
            "description_top_chunks": description_top_chunks,
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
