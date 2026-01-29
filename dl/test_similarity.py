from sentence_transformers import SentenceTransformer, util

## LLM via API ?

# Load the same model you use in Flask
model = SentenceTransformer("all-mpnet-base-v2")

# Two example descriptions (you can replace these)
desc1 = "Contains all sales transactions including customer IDs and purchase amounts."
# desc2 = "Outside it's sunny and a man is playing a guitar on the stage, near the hospital."
desc2 = "Includes purchase records linked to customer identifiers and total spend."

# Encode both descriptions (IN EMBEDDING VETTORIALI)
emb1 = model.encode([desc1], convert_to_tensor=True)
emb2 = model.encode([desc2], convert_to_tensor=True)

# Compute cosine similarity
similarity = util.cos_sim(emb1, emb2).item()

print(f"Cosine similarity: {similarity:.4f} ({similarity * 100:.2f}%)")

# Confrontare la somiglianza semantica tra due descrizioni di testo usando Sentence Transformers
# e calcolare la similarità coseno tra i loro vettori di embedding.

# Un embedding è una rappresentazione numerica (vettore) di un testo, in cui testi
# con significati simili avranno vettori vicini tra loro nello spazio.
# In pratica, trasforma le frasi in numeri in modo che possano essere confrontate.
