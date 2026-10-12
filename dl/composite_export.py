"""Exports for composite reports shared by Flask and FastAPI."""

from datetime import datetime, timezone
from typing import Any, Dict, List


def _profile_id(profile: Dict[str, Any]) -> str:
    return str(profile.get("id") or profile.get("@id") or profile.get("name") or "").strip()


def _profiles_from_report(report: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    profiles: Dict[str, Dict[str, Any]] = {}
    for composite in report.get("composites", []) or []:
        for profile in composite.get("profiles", []) or []:
            if not isinstance(profile, dict):
                continue
            profile_id = _profile_id(profile)
            if profile_id:
                profiles.setdefault(profile_id, profile)
    return profiles


def _add_edge(edges: List[Dict[str, Any]], edge_id: str, source: str, target: str, label: str, **properties):
    edges.append({
        "id": edge_id,
        "from": source,
        "to": target,
        "labels": [label],
        "properties": properties,
    })


def build_moma_composite_export(report: Dict[str, Any]) -> Dict[str, Any]:
    """Build a graph-shaped, forward-compatible composite export.

    The original report is retained under ``analysis`` so this format can be
    imported later without losing evidence that is not yet mapped to MoMa.
    """
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    profiles = _profiles_from_report(report)
    composite_items = report.get("final_composites") or report.get("composites") or []

    for profile_id, profile in profiles.items():
        nodes.append({
            "id": f"dataset:{profile_id}",
            "labels": ["Dataset", "sc:Dataset"],
            "properties": {
                "@type": "sc:Dataset",
                "@id": profile_id,
                "name": profile.get("name") or profile_id,
            },
        })

    elements = []
    for composite in composite_items:
        composite_id = str(composite.get("@id") or composite.get("id") or "").strip()
        if not composite_id:
            continue
        node_id = f"composite:{composite_id}"
        merge_evidence = composite.get("mergeEvidence") or {}
        element = {
            "@type": "CompositeDataLinkingElement",
            "@id": composite_id,
            "profiles": composite.get("profiles", []),
            "status": composite.get("status", "green"),
            "profile_count": composite.get("profile_count", len(composite.get("profiles", []) or [])),
            "composite_coherence": composite.get("composite_coherence", 0.0),
            "merge_similarity": composite.get("merge_similarity", 0.0),
            "chunk_similarity": merge_evidence.get("chunk_similarity", 0.0),
            "keyword_similarity": merge_evidence.get("keyword_similarity", 0.0),
            "evidence": composite.get("evidence", {}),
        }
        elements.append(element)
        nodes.append({
            "id": node_id,
            "labels": ["CompositeDataLinkingElement", "Composite"],
            "properties": {key: value for key, value in element.items() if key not in {"profiles", "evidence"}},
        })
        for index, profile in enumerate(composite.get("profiles", []) or []):
            profile_id = _profile_id(profile)
            if profile_id:
                _add_edge(
                    edges,
                    f"contains:{composite_id}:{profile_id}",
                    node_id,
                    f"dataset:{profile_id}",
                    "HAS_PROFILE",
                    role="member",
                    ordinal=index,
                )

    comparison_count = 0
    for comparison in report.get("recursive_comparisons", []) or []:
        comparison_id = str(comparison.get("@id") or "").strip()
        if not comparison_id:
            continue
        node_id = f"comparison:{comparison_id}"
        nodes.append({
            "id": node_id,
            "labels": ["CompositeComparison", "Comparison"],
            "properties": {
                "@type": "CompositeComparison",
                "@id": comparison_id,
                "status": comparison.get("status"),
                "recursive_level": comparison.get("recursive_level"),
                "relation_similarity": comparison.get("relation_similarity"),
                "merge_similarity": comparison.get("merge_similarity"),
                "chunk_similarity": comparison.get("chunk_similarity"),
                "keyword_similarity": comparison.get("keyword_similarity"),
                "merge_type": comparison.get("merge_type"),
            },
        })
        for side, reference in (("left", comparison.get("leftRef")), ("right", comparison.get("rightRef"))):
            if reference:
                _add_edge(edges, f"compares:{comparison_id}:{side}", node_id, f"composite:{reference}", "COMPARES", side=side)
        comparison_count += 1

    ai_summary = {
        "purpose": "Dataset Linking composite analysis",
        "source": report.get("source"),
        "profile_count": len(profiles),
        "composite_count": len(elements),
        "comparison_count": comparison_count,
        "status_counts": {
            "green": sum(1 for item in elements if item.get("status") == "green"),
            "red": sum(1 for item in elements if item.get("status") == "red"),
        },
        "interpretation": "Each composite groups dataset profiles connected by similarity links. Composite comparisons describe why groups were merged; chunk and keyword scores are normalized between 0 and 1.",
    }

    return {
        "@context": "http://mlcommons.org/croissant/",
        "@type": ["CompositeDataLinkingElementReport", "MoMaDatasetLinkingExport"],
        "format": {
            "name": "DataGEMS MoMa-compatible composite export",
            "version": "0.1",
            "graph_model": "PGJSON-compatible nodes and edges",
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": report.get("source"),
        "selection": report.get("selection", {}),
        "weights": report.get("weights", {}),
        "ai_summary": ai_summary,
        "nodes": nodes,
        "edges": edges,
        "elements": elements,
        "analysis": report,
    }
