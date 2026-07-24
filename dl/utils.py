# dl/utils.py
from pathlib import Path

CACHE_DIR = Path(__file__).parent / 'DLRepository'
CACHE_DIR.mkdir(exist_ok=True)

def get_float_arg(name: str, default: float) -> float:
    from flask import request

    raw = request.args.get(name, default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)

def get_weights_and_threshold():
    kw = get_float_arg("kw", 0.6)
    desc = get_float_arg("desc", 0.3)
    head = get_float_arg("head", 0.1)
    th = get_float_arg("th", 30)

    total = kw + desc + head
    normalized = False
    if total > 0 and abs(total - 1.0) > 1e-6:
        kw, desc, head = (kw / total, desc / total, head / total)
        normalized = True

    return kw, desc, head, th, normalized

def normalize_weights(kw: float, desc: float, head: float):
    total = kw + desc + head
    normalized = False
    if total > 0 and abs(total - 1.0) > 1e-6:
        kw, desc, head = (kw / total, desc / total, head / total)
        normalized = True
    return kw, desc, head, normalized

def normalize_keywords(keywords):
    """
    Clean and normalize keyword lists.
    Keep only strings, strip spaces, lowercase, and remove empty entries.
    For hierarchical keywords like "parent>child>leaf", keep all levels.
    Example:
        ["  Sales ", "Education>Math>Algebra", " ", None, "SALES", 123]
        --> {'sales', 'education', 'math', 'algebra'}
    """
    return set(_keyword_terms(keywords))


def keywords_to_text(keywords):
    """Build a single text string for SBERT keyword similarity."""
    return " ".join(_keyword_terms(keywords))


def _keyword_terms(keywords):
    if not keywords:
        return []
    if isinstance(keywords, str):
        keywords = [keywords]

    terms = []
    seen = set()
    for keyword in keywords:
        if not isinstance(keyword, str):
            continue
        for part in keyword.split(">"):
            term = part.strip().lower()
            if term and term not in seen:
                terms.append(term)
                seen.add(term)
    return terms

def get_DLRepository_path(folder, kw, desc, head):
    """Return the cache file path for a given folder and weight combination."""
    safe_folder = str(folder).replace("\\", "_").replace("/", "_").replace(":", "")
    key = f"{safe_folder}_kw{kw:.2f}_desc{desc:.2f}_head{head:.2f}.json"
    return CACHE_DIR / key

def classify_dataset(dp):
    """
    Classify a Croissant DataProfile into:
    - 'tabular'      → 1–2 tables with limited columns
    - 'relational'   → 3+ structured tables (multi-table dataset)
    - 'unstructured' → text corpora, materials, or field-less structures
    """

    distributions = dp.get("distribution", [])
    recordsets = dp.get("recordSet", [])

    # Number of tables
    n_rs = len(recordsets)

    # Number of fields per table
    fields_per_rs = [len(rs.get("field", [])) for rs in recordsets]

    # ----------------------------------------
    # RULE 1 — Unstructured datasets
    # ----------------------------------------

    # Case A: no fields at all (materials, ipynb, PDF collections)
    if any(n == 0 for n in fields_per_rs):
        return "unstructured"

    # Case B: very large recordsets → text corpora (Wikipedia, Diderot, 19th Century)
    if any(n > 1000 for n in fields_per_rs):
        return "unstructured"

    # ----------------------------------------
    # RULE 2 — Relational datasets
    # ----------------------------------------

    # Case C: many recordsets (≥ 3) → multi-table datasets
    # e.g., esco, cedefop, meteo_era5land, mathe_integration
    if n_rs >= 3:
        # At least one table must look like a real table (≥ 5 columns)
        if any(n >= 5 for n in fields_per_rs):
            return "relational"

    # Check SQL encoding → strongly relational
    for dist in distributions:
        encoding = dist.get("encodingFormat", "").lower()
        if "sql" in encoding:
            return "relational"

    # ----------------------------------------
    # RULE 3 — Tabular datasets
    # ----------------------------------------

    # Case D: 1–2 structured tables → classic tabular datasets
    if n_rs in (1, 2) and all(1 <= n <= 50 for n in fields_per_rs):
        return "tabular"

    # ----------------------------------------
    # DEFAULT — Unstructured (fallback)
    # ----------------------------------------
    return "unstructured"

