"""Demonstrate one shared chunk candidate per profile in a multi-profile group."""

from dl import link_comparison, similarity


DESCRIPTIONS = {
    "Weather observations": (
        "Daily temperature, rainfall and humidity observations from environmental stations. "
        "The records support climate monitoring and analysis of regional weather conditions."
    ),
    "Climate indicators": (
        "Monthly climate indicators describe temperature trends, rainfall patterns and humidity. "
        "The data supports climate impact analysis across monitored regions."
    ),
    "Air quality": (
        "Environmental stations measure air pollution together with temperature, rainfall and humidity. "
        "The observations support analysis of atmospheric and climate conditions."
    ),
    "Drought indicators": (
        "Drought indicators combine rainfall deficit, soil moisture and temperature anomalies. "
        "The records support climate impact assessment and environmental monitoring."
    ),
    "River water quality": (
        "River monitoring stations record water quality, rainfall, water temperature and humidity. "
        "The measurements support watershed and environmental condition analysis."
    ),
    "Urban transport": (
        "Daily urban mobility demand includes public transport usage, passenger flows and travel time. "
        "The data supports traffic analysis and city infrastructure planning."
    ),
    "Energy consumption": (
        "Monthly energy demand includes electricity consumption, peak load and infrastructure indicators. "
        "The data supports urban planning and sustainable city management."
    ),
    "Public mobility demand": (
        "Public mobility measurements describe passenger demand, service capacity and travel time. "
        "The records support urban transport planning and infrastructure evaluation."
    ),
    "City infrastructure": (
        "Urban infrastructure indicators describe service demand, transport pressure and infrastructure capacity. "
        "The dataset supports integrated city planning and public service analysis."
    ),
    "Smart building energy": (
        "Smart building records measure energy demand, electricity consumption and infrastructure load. "
        "The data supports urban energy management and infrastructure planning."
    ),
}


def show_group(title, names):
    chunks = {
        name.lower(): similarity._split_chunks(DESCRIPTIONS[name])
        for name in names
    }
    labels = {name.lower(): name for name in names}
    matches = link_comparison._multi_profile_chunk_evidence(
        list(chunks), chunks, labels, limit=3
    )

    print(f"\n{title}")
    print("=" * len(title))
    for match in matches:
        print(f"\nEvidence {match['rank']} - score: {match['similarity']:.1%}")
        for item in match["profiles"]:
            print(f"- {item['profile']}: {item['text']}")


def main():
    print(f"Loaded {len(DESCRIPTIONS)} descriptions.")
    show_group(
        "Environment and climate group",
        [
            "Weather observations",
            "Climate indicators",
            "Air quality",
            "Drought indicators",
            "River water quality",
        ],
    )
    show_group(
        "Urban mobility and energy group",
        [
            "Urban transport",
            "Energy consumption",
            "Public mobility demand",
            "City infrastructure",
            "Smart building energy",
        ],
    )


if __name__ == "__main__":
    main()
