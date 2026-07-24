import json
import pandas as pd
import torch
import os
import shutil
import glob
from pathlib import Path
import dl.similarity as sim
from dl.refine import split_chunks


def export_to_excel_from_api(output_name="Similarity_Report.xlsx"):
    # 1. Clean cache and temporary folders.
    print("🧹 Cleaning cache and temporary files...")
    for cache_file in glob.glob("cache_local_*.json"):
        os.remove(cache_file)

    temp_folder = Path("temp_api_profiles")
    if temp_folder.exists(): shutil.rmtree(temp_folder)
    temp_folder.mkdir(exist_ok=True)

    # 2. Download and filter profiles.
    print("🌐 Downloading and filtering profiles from API...")
    raw_profiles = sim.fetch_profiles_from_api()

    # fetch_profiles_from_api already filters real ready profiles and keeps the
    # most complete profile when multiple datasets share the same name.
    profiles_data = {
        ds_id: data
        for ds_id, data in raw_profiles.items()
        if str(data.get("status", "")).lower() == "ready"
    }

    if len(profiles_data) < 2:
        print(
            f"⚠️ Error: found only {len(profiles_data)} datasets with status 'ready'. At least 2 are required for comparison.")
        return

    # JSON serialization helper.
    def json_serializer(obj):
        if isinstance(obj, set): return list(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    for ds_id, data in profiles_data.items():
        clean_id = ds_id.replace(".json", "")
        with open(temp_folder / f"{clean_id}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, default=json_serializer)

    print(f"🔄 Analysis started for {len(profiles_data)} 'ready' profiles...")

    # Print analyzed profiles for debugging.
    print("\n--- Profiles under analysis ---")
    for ds_id, data in profiles_data.items():
        print(f"ID: {ds_id.replace('.json', '')} | Name: {data.get('name', 'N/A')}")
    print(f"--- Total: {len(profiles_data)} ---\n")

    # 3. Run similarity engine.
    error, similarities, _ = sim.compute_similarities(
        folder_path=str(temp_folder),
        kw_weight=0.33, desc_weight=0.33, head_weight=0.33,
        threshold=0, use_api=False
    )

    if error:
        print(f"❌ Error during analysis: {error}")
        return

    sim._ensure_models()
    report_rows = []

    # Chunk helper.
    def get_top_chunks(text1, text2):
        chunks1, chunks2 = split_chunks(text1), split_chunks(text2)
        if not chunks1 or not chunks2: return "N/A"

        emb1 = sim._model_long.encode(chunks1, convert_to_tensor=True)
        emb2 = sim._model_long.encode(chunks2, convert_to_tensor=True)
        scores = torch.nn.functional.cosine_similarity(emb1.unsqueeze(1), emb2.unsqueeze(0), dim=2)

        matches = []
        for i, c in enumerate(chunks1):
            best_idx = scores[i].argmax().item()
            matches.append((c, chunks2[best_idx], scores[i].max().item()))

        top3 = sorted(matches, key=lambda x: x[2], reverse=True)[:3]
        return " | ".join([f"[{m[2]:.2f}] {m[0]} <-> {m[1]}" for m in top3])

    # 4. Build report rows.
    for s in similarities:
        id1 = s["id1"].replace(".json", "")
        id2 = s["id2"].replace(".json", "")

        # Read data from the temporary files just created.
        try:
            p1 = json.loads((temp_folder / f"{id1}.json").read_text(encoding="utf-8"))
            p2 = json.loads((temp_folder / f"{id2}.json").read_text(encoding="utf-8"))
        except FileNotFoundError:
            continue

        report_rows.append({
            "ID dataset 1": id1,
            "Dataset 1": p1.get("name", s["dataprofile1"]),
            "ID dataset 2": id2,
            "Dataset 2": p2.get("name", s["dataprofile2"]),
            "Common Keywords": s["common_keywords"],
            "Unique to DS1": s["unique_to_1"],
            "Unique to DS2": s["unique_to_2"],
            "Top 3 Desc Chunks": get_top_chunks(p1.get("description", ""), p2.get("description", "")),
            "Top 3 Headline Chunks": get_top_chunks(p1.get("headline", ""), p2.get("headline", "")),
            "Sim Keywords (%)": s["keywords_similarity"],
            "Sim Description (%)": s["description_similarity"],
            "Sim Headline (%)": s["headline_similarity"],
            "Final Sim (%)": s["combined_similarity"]
        })

    # 5. Export.
    df = pd.DataFrame(report_rows)
    df.to_excel(output_name, index=False)

    shutil.rmtree(temp_folder)
    print(f"✅ Report ready: {output_name}")


if __name__ == "__main__":
    export_to_excel_from_api()
