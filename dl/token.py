import requests
import json
import sys
import io
import os
import re
import urllib3
from typing import Any

# --- CONFIGURAZIONE SICUREZZA ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- FIX ENCODING TERMINALE ---
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE_URL = "https://datagems-dev.scayle.es"
OUTPUT_FOLDER = "profiles_api"

def fix_encoding(data: Any) -> Any:
    """Corregge eventuali problemi di encoding nelle stringhe ricevute"""
    if isinstance(data, str):
        try:
            return data.encode('latin-1').decode('utf-8')
        except:
            return data
    elif isinstance(data, list):
        return [fix_encoding(item) for item in data]
    elif isinstance(data, dict):
        return {k: fix_encoding(v) for k, v in data.items()}
    return data

def get_token() -> str:
    """Ottiene il token OAuth2 bypassando SSL"""
    url = f"{BASE_URL}/oauth/realms/dev/protocol/openid-connect/token"
    payload = {
        'grant_type': 'password',
        'client_id': 'swagger-client',
        'username': 'dg-user-admin',
        'password': 'dg-user-admin',
        'scope': 'datagems'
    }
    response = requests.post(url, data=payload, timeout=15, verify=False)
    if response.status_code != 200:
        print(f"❌ ERRORE AUTENTICAZIONE ({response.status_code})")
        sys.exit(1)
    return response.json().get('access_token')

def get_dataset_detail(token: str, dataset_id: str) -> dict:
    """Scarica il profilo Croissant di un singolo dataset"""
    url = f"{BASE_URL}/dmm/api/v1/dataset/get/{dataset_id}?format=croissant"
    headers = {'accept': 'application/json', 'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, timeout=25, verify=False)
        response.raise_for_status()
        return fix_encoding(response.json()).get('dataset', {})
    except Exception as e:
        print(f"\n⚠️ Errore nel download dell'ID {dataset_id}: {e}")
        return {}

def sanitize_filename(name: str) -> str:
    """Rimuove caratteri non validi per i nomi file"""
    clean_name = re.sub(r'[\\/*?:"<>|]', "", name)
    return clean_name.replace(" ", "_")

if __name__ == "__main__":
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    access_token = get_token()
    print("🔑 Token ottenuto con successo.")

    # --- AGGIORNAMENTO: Aggiunto count=200 per superare il limite di 25 ---
    search_url = f"{BASE_URL}/dmm/api/v1/dataset/search"
    params = {'properties': 'name', 'count': 200, 'dataset_status': 'ready'}
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        response = requests.get(search_url, headers=headers, params=params, timeout=30, verify=False)
        response.raise_for_status()
        all_datasets = response.json().get('datasets', [])
    except Exception as e:
        print(f"❌ Impossibile recuperare la lista dataset: {e}")
        sys.exit(1)

    total = len(all_datasets)
    print(f"📂 Trovati {total} dataset. Inizio il download...\n")

    for index, item in enumerate(all_datasets, 1):
        nodes = item.get('nodes', [])
        if not nodes: continue

        ds_id = nodes[0].get('id')
        ds_name = nodes[0].get('properties', {}).get('name', "unnamed")

        clean_name = sanitize_filename(ds_name)
        filename = f"{clean_name}_{ds_id}.json"
        filepath = os.path.join(OUTPUT_FOLDER, filename)

        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                details = json.load(f)
            status = details.get('status', 'unknown')
            print(f"[{index}/{total}] ⏩ Saltato (esistente): {filename} | ℹ️  Status: {status}")
            continue

        details = get_dataset_detail(access_token, ds_id)

        if details:
            ds_status = details.get('status', 'N/A')
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(details, f, indent=4, ensure_ascii=False)
            status_icon = "🟢" if ds_status == "ready" else "🟡"
            print(f"[{index}/{total}] ✅ Salvato: {filename} | {status_icon} Status: {ds_status}")
        else:
            print(f"[{index}/{total}] ❌ Fallito: {ds_name}")

    print(f"\n✅ Operazione completata. File in: {os.path.abspath(OUTPUT_FOLDER)}")