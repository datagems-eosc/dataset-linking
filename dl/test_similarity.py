import re
import torch
from sentence_transformers import SentenceTransformer, util


def split_chunks(text: str) -> list[str]:
    """Divide il testo in segmenti logici basati sulla punteggiatura o congiunzioni."""
    """Text divided into logical segments"""
    chunks = re.split(r"[,.:;]| and | or | but ", text)
    return [chunk.strip() for chunk in chunks if len(chunk.strip()) > 3] #vengono considerati solo i segmenti da almeno 3 caratteri


def print_chunk_explainability(model: SentenceTransformer, text1: str, text2: str) -> None:
    """Analizza quali parti della Descrizione 1 trovano riscontro nella Descrizione 2."""
    chunks1 = split_chunks(text1)
    chunks2 = split_chunks(text2)

    if not chunks1 or not chunks2:
        print("\nToken analysis impossibile: text too short.")
        return

    emb1 = model.encode(chunks1, convert_to_tensor=True, normalize_embeddings=True)
    emb2 = model.encode(chunks2, convert_to_tensor=True, normalize_embeddings=True)

    sim_matrix = util.cos_sim(emb1, emb2)

    # Lista per memorizzare tutti i match per poterli ordinare alla fine
    all_matches = []

    print("\n--- Semantic anlysis by chunk ---")
    print(f"Description 1 is composed of {len(chunks1)} logical segments.")

    for idx, chunk in enumerate(chunks1):
        best_match_idx = torch.argmax(sim_matrix[idx]).item()
        best_score = sim_matrix[idx][best_match_idx].item()

        # Salviamo il match nella lista
        all_matches.append({
            'chunk1': chunk,
            'chunk2': chunks2[best_match_idx],
            'score': best_score
        })

        status = "✅" if best_score > 0.85 else "⚠️" if best_score > 0.65 else "❌"
        print(f'{status} Chunk: "{chunk}"')
        print(f'   Best matching: "{chunks2[best_match_idx]}" ({best_score:.2%})')
        print("-" * 30)

    # --- NUOVA SEZIONE: TOP 3 MATCHES ---
    print("\n> Top 5 chunks:")
    # Ordiniamo la lista in base allo score in modo decrescente
    top_5 = sorted(all_matches, key=lambda x: x['score'], reverse=True)[:5]

    for i, match in enumerate(top_5, 1):
        print(f"{i}. Score: {match['score']:.2%}")
        print(f"   D1: \"{match['chunk1']}\"")
        print(f"   D2: \"{match['chunk2']}\"")
        print()


def explain_similarity(score: float) -> str:
    """Fornisce un'interpretazione testuale del punteggio finale."""
    if score >= 0.85: return "Very High similarity"
    if score >= 0.70: return "High similarity"
    if score >= 0.50: return "Medium moderata"
    return "Low similarity"


# --- ESECUZIONE ---
#model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
model = SentenceTransformer("all-mpnet-base-v2")

desc1 = "This dataset includes data on price for rent in Barcelona, Spain. The data was collected for a period of 2014 - 2022 years, divided into trimesters. The prices go by neighbourhoods and districts. This dataset includes both prices per month and prices per square meter, so that you can easier compare them."
desc2 = "This dataset compiles detailed records of traffic accidents in Barcelona, collected from annual reports by the Guardia Urbana and made available via OpenDataBCN. It includes over 110,000 observations spanning multiple years, capturing comprehensive information about each incident. Key attributes include unique identifiers for each case, district, neighborhood, and street, alongside location details such as postal codes and geographic coordinates (latitude, longitude, and UTM). Temporal data cover the exact year, month, day, hour, and part of the day when accidents occurred. The dataset also records the cause of each accident and the resulting human impact, including numbers of deaths, severely and mildly wounded, and total victims. Vehicle involvement counts are provided as well. This rich combination of spatial, temporal, and incident-specific data allows for in-depth analysis of accident patterns, enabling studies on risk factors, hotspot identification, and temporal trends. The dataset is ideal for urban planners, traffic safety analysts, and policymakers aiming to improve road safety and reduce accidents in Barcelona."
emb_total1 = model.encode(desc1, convert_to_tensor=True)
emb_total2 = model.encode(desc2, convert_to_tensor=True)
global_sim = util.cos_sim(emb_total1, emb_total2).item()

print(f"Global similarity: {global_sim:.2%}")
print(f"Explaination: {explain_similarity(global_sim)}")

print_chunk_explainability(model, desc1, desc2)

# NEL BASIC DL-ELEMENT HA SENSO AGGIUNGERE I PESI
# I 3 VIOLA VANNO BENE COME SONO, SONO