import unittest

from dl.composite_export import build_moma_composite_export


class CompositeExportTests(unittest.TestCase):
    def test_moma_export_contains_graph_and_original_analysis(self):
        report = {
            "@context": "http://mlcommons.org/croissant/",
            "@type": "CompositeDataLinkingElementReport",
            "source": "demo_profiles",
            "selection": {"links_considered": 2},
            "weights": {"keywords": 0.6},
            "composites": [
                {
                    "@id": "composite:1",
                    "profiles": [{"id": "A", "name": "Dataset A"}, {"id": "B", "name": "Dataset B"}],
                    "status": "green",
                    "profile_count": 2,
                    "composite_coherence": 0.8,
                    "merge_similarity": 0.75,
                    "mergeEvidence": {"chunk_similarity": 0.8, "keyword_similarity": 0.6},
                }
            ],
            "recursive_comparisons": [
                {
                    "@id": "recursive-comparison:1",
                    "leftRef": "composite:left",
                    "rightRef": "composite:right",
                    "status": "green",
                    "merge_similarity": 0.75,
                }
            ],
        }

        exported = build_moma_composite_export(report)

        self.assertIn("MoMaDatasetLinkingExport", exported["@type"])
        self.assertEqual(exported["format"]["graph_model"], "PGJSON-compatible nodes and edges")
        self.assertEqual(exported["source"], "demo_profiles")
        self.assertIs(exported["analysis"], report)
        self.assertTrue(any("Dataset" in node["labels"] for node in exported["nodes"]))
        self.assertTrue(any(edge["labels"] == ["HAS_PROFILE"] for edge in exported["edges"]))
        self.assertEqual(exported["ai_summary"]["composite_count"], 1)


if __name__ == "__main__":
    unittest.main()
