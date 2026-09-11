import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dl import flask_app
from dl.flask_app import _compact_composite_report


def _recursive_comparison(index):
    large_scores = [
        {"left_text": "x" * 500, "right_text": "y" * 500, "similarity": 0.8}
        for _ in range(100)
    ]
    return {
        "@id": f"recursive:{index}",
        "recursive_level": 3,
        "leftRef": "left",
        "rightRef": "right",
        "merge_type": "shared_profile_bridge",
        "merge_similarity": 0.8,
        "chunk_similarity": 0.85,
        "keyword_similarity": 0.65,
        "applied_threshold": 0.5,
        "evidence": {
            "chunk_evidence_strategy": "all_original_profile_chunks_cached",
            "evaluated_profile_pair_count": 12,
            "description_chunk_matches": [
                {
                    "rank": rank,
                    "similarity": 0.9 - rank / 100,
                    "left_profile_key": "a",
                    "right_profile_key": "b",
                    "chunk_evidence_1": "left evidence",
                    "chunk_evidence_2": "right evidence",
                    "valid_atomic_pair_count": 40,
                }
                for rank in range(1, 6)
            ],
            "cross_profile_pair_scores": large_scores,
            "excluded_self_chunk_pairs": large_scores,
        },
    }


class CompactCompositeReportTests(unittest.TestCase):
    def test_compact_report_keeps_results_without_repeated_heavy_evidence(self):
        supporting = [_recursive_comparison(index) for index in range(25)]
        composite = {
            "@id": "composite:1",
            "profiles": [{"id": "A", "name": "A"}, {"id": "B", "name": "B"}],
            "profile_count": 2,
            "link_count": 1,
            "comparison_count": 4,
            "recursive_level": 3,
            "composite_coherence": 0.8,
            "merge_similarity": 0.82,
            "representative_keywords": ["math", "education"],
            "supportingComparisons": supporting,
            "mergeEvidence": {
                "merge_type": "shared_profile_bridge",
                "chunk_similarity": 0.85,
                "keyword_similarity": 0.65,
                "chunk_evidence_strategy": "all_original_profile_chunks_cached",
                "evaluated_profile_pair_count": 12,
                "description_chunk_matches": supporting[0]["evidence"]["description_chunk_matches"],
            },
            "evidence": {
                "shared_keywords": ["math", "education"],
                "file_table_names": ["data.csv"],
                "csv_table_column_names": ["score"],
            },
        }
        report = {
            "@context": "context",
            "@type": "CompositeDataLinkingElementReport",
            "source": "test",
            "selection": {"composite_construction": "recursive_original_chunk_keyword_with_latent_bridges"},
            "weights": {"keywords": 0.6},
            "levels": [{"level": 1, "name": "Links", "items": [composite]}],
            "recursive_levels": [{"level": 1, "items": [composite]}],
            "comparisons": supporting,
            "recursive_comparisons": supporting,
            "connected_component_composites": [composite],
            "connected_component_levels": [{"level": 1, "items": [composite]}],
            "composites": [composite],
        }

        compact = _compact_composite_report(report)
        compact_composite = compact["composites"][0]

        self.assertEqual(compact["download_projection"]["mode"], "compact")
        self.assertEqual(compact_composite["supporting_path_count"], 25)
        self.assertEqual(compact_composite["supporting_paths_included"], 10)
        self.assertEqual(len(compact_composite["top_chunk_matches"]), 3)
        self.assertNotIn("comparisons", compact)
        self.assertNotIn("recursive_levels", compact)
        self.assertLess(
            len(json.dumps(compact, separators=(",", ":"))),
            len(json.dumps(report)) / 10,
        )

        with tempfile.TemporaryDirectory() as directory:
            with patch.object(flask_app, "COMPOSITE_DOWNLOAD_DIR", Path(directory)):
                token = flask_app._cache_compact_composite_report(report)
                cached_path = Path(directory) / f"{token}.json"
                self.assertTrue(cached_path.is_file())

                with patch.object(
                    flask_app,
                    "compute_similarities",
                    side_effect=AssertionError("download must not recalculate"),
                ):
                    response = flask_app.app.test_client().get(f"/composite_download/{token}")

                self.assertEqual(response.status_code, 200)
                self.assertEqual(json.loads(response.data), compact)
                response.close()


if __name__ == "__main__":
    unittest.main()
