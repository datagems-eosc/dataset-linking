import json
import re
import os
from pathlib import Path
from itertools import combinations
from datetime import datetime, timezone

# ====================================================================
# CONFIGURAZIONE PERCORSI
# ====================================================================
# Ho impostato il percorso specifico che hai indicato
DATA_FOLDER = r"C:\Users\tanfo\PycharmProjects\dataLinking\dl\profiles_api"
OUTPUT_FILE = "risultati_refinement.json"


# ====================================================================
# LOGICA DI ANALISI (REFINE)
# ====================================================================

def normalize_name(name: str) -> str:
    """Rimuove spazi, underscore e trattini per match robusti."""
    if not name: return ""
    return re.sub(r'[_ \-]', '', str(name)).lower()


def infer_content_type(profile_data: dict) -> str:
    dist = profile_data.get("distribution", []) or []
    # Controllo Database Connection
    if any("DatabaseConnection" in str(d.get("@type")) for d in dist):
        return "SQL"

    formats = {str(d.get("encodingFormat")).lower() for d in dist if d.get("encodingFormat")}
    if any("pdf" in f for f in formats): return "MIXED/TEXTUAL"
    if any("csv" in f or "sql" in f or "excel" in f for f in formats): return "TABULAR"

    if profile_data.get("recordSet"): return "TABULAR/EXTRACTED"
    return "UNKNOWN"


def extract_structural_info(profile_data: dict):
    """Estrae i nomi dei file PDF e le colonne delle tabelle (con campioni)."""
    docs = set()
    tables = []
    all_cols_norm = {}  # norm_name -> original_name

    for rs in profile_data.get("recordSet", []) or []:
        rs_name = rs.get("name", "unnamed")

        # 1. Identificazione File Documentali
        if rs_name.lower().endswith((".pdf", ".txt", ".doc")):
            docs.add(rs_name.lower())
            continue

        # 2. Identificazione Tabelle e Colonne
        cols_map = {}

        # A. Estrazione dai Field
        for field in rs.get("field", []) or []:
            orig = field.get("name")
            if orig:
                norm = normalize_name(orig)
                samples = set(str(s).strip().lower() for s in (field.get("sample") or []))
                cols_map[norm] = {"name": orig, "samples": samples}
                all_cols_norm[norm] = orig

        # B. Estrazione da Examples (Fallback per nuovi formati)
        if rs.get("examples"):
            try:
                ex_data = json.loads(rs["examples"])
                first_entry = {}
                if isinstance(ex_data, list) and ex_data:
                    first_entry = ex_data[0]
                elif isinstance(ex_data, dict):
                    first_entry = ex_data

                for k, v in first_entry.items():
                    norm = normalize_name(k)
                    if norm not in cols_map:
                        cols_map[norm] = {"name": k, "samples": set()}
                        all_cols_norm[norm] = k

                    # Recupero campioni se presenti come lista
                    if isinstance(ex_data, dict) and isinstance(ex_data[k], list):
                        cols_map[norm]["samples"].update(str(x).strip().lower() for x in ex_data[k])
                    elif v:
                        cols_map[norm]["samples"].add(str(v).strip().lower())
            except:
                pass

        if cols_map:
            tables.append({"name": rs_name, "columns": cols_map})

    return {
        "docs": docs,
        "tables": tables,
        "all_cols_norm": all_cols_norm,
        "keywords": set(profile_data.get("keywords", []))
    }


def compare_profiles(p1_info, p2_info):
    """Confronta due profili estratti cercando intersezioni."""
    # Match Documenti
    common_docs = sorted(list(p1_info["docs"] & p2_info["docs"]))
    # Match Keywords
    common_kw = sorted(list(p1_info["keywords"] & p2_info["keywords"]))

    # Match Colonne
    norms1 = set(p1_info["all_cols_norm"].keys())
    norms2 = set(p2_info["all_cols_norm"].keys())
    common_norms = sorted(list(norms1 & norms2))

    overlaps = []
    for norm in common_norms:
        s1, s2 = set(), set()
        for t in p1_info["tables"]:
            if norm in t["columns"]: s1.update(t["columns"][norm]["samples"])
        for t in p2_info["tables"]:
            if norm in t["columns"]: s2.update(t["columns"][norm]["samples"])

        common_vals = sorted(list(s1 & s2))
        overlaps.append({
            "colonna": p1_info["all_cols_norm"][norm],
            "comuni": common_vals,
            "match": "HIGH" if common_vals else "SCHEMA_ONLY"
        })

    return {
        "files": common_docs,
        "keywords": common_kw,
        "colonne": [p1_info["all_cols_norm"][n] for n in common_norms],
        "dati": overlaps
    }


# ====================================================================
# ESECUZIONE
# ====================================================================

def run_analysis():
    path = Path(DATA_FOLDER)
    if not path.exists():
        print(f"❌ Errore: La cartella {DATA_FOLDER} non esiste.")
        return

    json_files = list(path.glob("*.json"))
    print(f"🔍 Trovati {len(json_files)} file in {DATA_FOLDER}")

    # 1. Estrazione dati da tutti i file
    all_extracted = []
    for f in json_files:
        try:
            with open(f, 'r', encoding='utf-8') as jf:
                data = json.load(jf)
                all_extracted.append({
                    "filename": f.name,
                    "display_name": data.get("name", f.name),
                    "type": infer_content_type(data),
                    "info": extract_structural_info(data)
                })
        except Exception as e:
            print(f"⚠️ Errore nel leggere {f.name}: {e}")

    # 2. Confronto a coppie
    final_results = []
    for p1, p2 in combinations(all_extracted, 2):
        res = compare_profiles(p1["info"], p2["info"])

        # Filtro: salviamo solo se c'è almeno un match
        if res["files"] or res["colonne"] or res["keywords"]:
            final_results.append({
                "coppia": [p1["display_name"], p2["display_name"]],
                "tipi": [p1["type"], p2["type"]],
                "confronto": res
            })

    # 3. Salvataggio
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        json.dump(final_results, out, indent=4)

    print(f"✅ Analisi completata! Risultati salvati in: {os.path.abspath(OUTPUT_FILE)}")
    print(f"📊 Totale connessioni trovate: {len(final_results)}")


if __name__ == "__main__":
    run_analysis()