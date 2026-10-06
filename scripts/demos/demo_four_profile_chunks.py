"""Explain multi-profile chunk evidence for a four-profile composite.

This is an educational demo. It does not change the production composite
builder. It evaluates every chunk combination for four short descriptions and
prints the three strongest groups after checking all six pairwise similarities.
"""

from itertools import combinations, product
import sys
from pathlib import Path

from sentence_transformers import util

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dl import similarity


DESCRIPTIONS = {
    "A": (
        "Daily weather stations measure temperature, rainfall and humidity. "
        "These observations support climate monitoring and comparison of regional weather conditions."
    ),
    "B": (
        "Monthly climate indicators describe temperature trends, rainfall patterns and humidity. "
        "The indicators support analysis of climate conditions across regions."
    ),
    "C": (
        "Drought monitoring combines rainfall deficit, soil moisture and temperature anomalies. "
        "The indicators support climate impact assessment and environmental analysis."
    ),
    "D": (
        "River stations record water temperature, rainfall and humidity context. "
        "The measurements support environmental monitoring and analysis of regional climate conditions."
    ),
}


def _score_group(selected, embeddings):
    pair_scores = []
    details = []
    for left, right in combinations(sorted(selected), 2):
        left_index, left_text = selected[left]
        right_index, right_text = selected[right]
        score = float(util.cos_sim(
            embeddings[left][left_index],
            embeddings[right][right_index],
        ).item())
        score = max(0.0, min(1.0, score))
        pair_scores.append(score)
        details.append({
            "left": left,
            "right": right,
            "score": score,
            "left_text": left_text,
            "right_text": right_text,
        })

    minimum = min(pair_scores)
    average = sum(pair_scores) / len(pair_scores)
    group_score = 0.5 * minimum + 0.5 * average
    return group_score, minimum, average, details


def find_top_groups(limit=3):
    chunks = {
        profile: similarity._split_chunks(description)
        for profile, description in DESCRIPTIONS.items()
    }
    similarity._ensure_models()
    embeddings = {
        profile: similarity._model_long.encode(values, convert_to_tensor=True)
        for profile, values in chunks.items()
    }

    candidates = []
    for indexes in product(*(range(len(chunks[profile])) for profile in DESCRIPTIONS)):
        selected = {
            profile: (index, chunks[profile][index])
            for profile, index in zip(DESCRIPTIONS, indexes)
        }
        score, minimum, average, details = _score_group(selected, embeddings)
        candidates.append({
            "score": score,
            "minimum": minimum,
            "average": average,
            "selected": selected,
            "details": details,
        })

    candidates.sort(key=lambda item: item["score"], reverse=True)
    unique = []
    seen = set()
    for candidate in candidates:
        key = tuple(
            (profile, values[1])
            for profile, values in candidate["selected"].items()
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
        if len(unique) == limit:
            break
    return chunks, len(candidates), unique


def main():
    chunks, candidate_count, results = find_top_groups(limit=3)

    print("Four-profile multi-chunk evidence demo")
    print("========================================")
    print("Original descriptions: ")
    print("A: " + DESCRIPTIONS["A"])
    print("B: " + DESCRIPTIONS["B"])
    print("C: " + DESCRIPTIONS["C"])
    print("D: " + DESCRIPTIONS["D"])
    print("Profiles: A, B, C and D")
    print(f"Candidate combinations evaluated: {candidate_count}")
    print("Every candidate is checked through all six pairs: AB, AC, AD, BC, BD and CD.")

    for rank, result in enumerate(results, 1):
        print(f"\nTOP {rank}")
        print(f"Group score: {result['score']:.1%}")
        print(f"Minimum pair score: {result['minimum']:.1%}")
        print(f"Average pair score: {result['average']:.1%}")
        print("Selected chunks:")
        for profile, (_, text) in result["selected"].items():
            print(f"  {profile}: {text}")
        print("Pairwise reasoning:")
        for detail in result["details"]:
            print(
                f"  {detail['left']}-{detail['right']}: "
                f"{detail['score']:.1%}"
            )


if __name__ == "__main__":
    main()
