import sys
import types
import unittest
from unittest.mock import patch

import torch

import dl
from dl import link_comparison


def _item(item_id, profile_ids, link_ref):
    return {
        "id": item_id,
        "item_type": "DataLinkingBase",
        "linkRefs": [link_ref],
        "originRefs": [link_ref],
        "profiles": [{"id": profile_id, "name": profile_id} for profile_id in profile_ids],
        "representative_chunks": [],
        "keyword_evidence": {},
    }


class _FakeModel:
    def __init__(self):
        self.calls = []
        self.vectors = {
            "a-old": [1.0, 0.0],
            "a-hidden": [0.0, 1.0],
            "b-text": [1.0, 0.0],
            "c-text": [0.0, 1.0],
        }

    def encode(self, texts, convert_to_tensor=True):
        self.calls.append(tuple(texts))
        return torch.tensor([self.vectors[text] for text in texts])


class OriginalChunkComparisonTests(unittest.TestCase):
    def test_registry_keeps_chunks_outside_link_top_three(self):
        link = {
            "@id": "AB",
            "dataprofile1ref": "A",
            "dataprofile2ref": "B",
            "dp1Name": "A",
            "dp2Name": "B",
            "description_top_chunks": [
                {"chunk1": "first", "chunk2": "one", "similarity": 90},
                {"chunk1": "second", "chunk2": "two", "similarity": 80},
                {"chunk1": "third", "chunk2": "three", "similarity": 70},
            ],
            "_description_chunks_by_profile": {
                "A": {"id": "A", "name": "A", "chunks": ["first", "second", "third", "hidden fourth"]},
                "B": {"id": "B", "name": "B", "chunks": ["one", "two", "three"]},
            },
        }

        registry = link_comparison._original_chunk_registry([link])

        self.assertEqual([entry["text"] for entry in registry["a"]], [
            "first", "second", "third", "hidden fourth",
        ])

    def test_profile_pair_cache_reuses_profile_embeddings(self):
        registry = {
            "a": [
                {"text": "a-old", "profile_id": "A", "profile_name": "A", "profile_key": "a"},
                {"text": "a-hidden", "profile_id": "A", "profile_name": "A", "profile_key": "a"},
            ],
            "b": [{"text": "b-text", "profile_id": "B", "profile_name": "B", "profile_key": "b"}],
            "c": [{"text": "c-text", "profile_id": "C", "profile_name": "C", "profile_key": "c"}],
        }
        cache = {}
        model = _FakeModel()
        fake_similarity = types.SimpleNamespace(_ensure_models=lambda: None, _model_long=model)
        fake_sentence_transformers = types.SimpleNamespace(
            util=types.SimpleNamespace(
                cos_sim=lambda left, right: torch.nn.functional.normalize(left) @ torch.nn.functional.normalize(right).T
            )
        )

        with patch.object(dl, "similarity", fake_similarity, create=True):
            with patch.dict(sys.modules, {
                "dl.similarity": fake_similarity,
                "sentence_transformers": fake_sentence_transformers,
            }):
                first = link_comparison._profile_pair_original_chunk_match("a", "b", registry, cache)
                second = link_comparison._profile_pair_original_chunk_match("a", "c", registry, cache)

        self.assertEqual(first["left_evidence"]["text"], "a-old")
        self.assertEqual(second["left_evidence"]["text"], "a-hidden")
        self.assertEqual(model.calls.count(("a-old", "a-hidden")), 1)
        self.assertEqual(len(model.calls), 3)

    def test_original_comparison_keeps_one_best_match_per_profile_pair(self):
        left = _item("left", ["A", "B"], "AB")
        right = _item("right", ["B", "C", "D"], "BCD")
        registry = {key: [{"text": key, "profile_key": key}] for key in "abcd"}
        scores = {
            ("a", "b"): 0.70,
            ("a", "c"): 0.95,
            ("a", "d"): 0.80,
            ("b", "c"): 0.90,
            ("b", "d"): 0.85,
        }

        def fake_match(left_profile, right_profile, unused_registry, unused_cache):
            score = scores[tuple(sorted((left_profile, right_profile)))]
            return {
                "left_evidence": {"text": left_profile, "profile_key": left_profile},
                "right_evidence": {"text": right_profile, "profile_key": right_profile},
                "similarity": score,
                "evaluated_chunk_pair_count": 1,
            }

        with patch.object(link_comparison, "_profile_pair_original_chunk_match", side_effect=fake_match):
            result = link_comparison._compare_original_profile_chunks(left, right, registry, {})

        self.assertEqual(result["represented_left_profiles"], ["a", "b"])
        self.assertEqual(result["represented_right_profiles"], ["b", "c", "d"])
        self.assertEqual(result["evaluated_profile_pair_count"], 5)
        self.assertEqual([match["similarity"] for match in result["matches"]], [0.95, 0.90, 0.85])
        self.assertEqual(result["similarity"], 0.9)

    def test_builder_uses_original_chunks_for_recursive_merge(self):
        def link(link_id, left, right):
            return {
                "@id": link_id,
                "dataprofile1ref": left,
                "dataprofile2ref": right,
                "dp1Name": left,
                "dp2Name": right,
                "metrics": {"combined_similarity": 80},
                "common_keywords": "math",
                "description_top_chunks": [],
                "_description_chunks_by_profile": {
                    left: {"id": left, "name": left, "chunks": [f"{left}-original"]},
                    right: {"id": right, "name": right, "chunks": [f"{right}-original"]},
                },
            }

        def fake_match(left_profile, right_profile, registry, unused_cache):
            return {
                "left_evidence": registry[left_profile][0],
                "right_evidence": registry[right_profile][0],
                "similarity": 0.9,
                "evaluated_chunk_pair_count": 1,
            }

        with patch.object(link_comparison, "compare_datalinkingbase_links", return_value=[]), patch.object(
            link_comparison,
            "_profile_pair_original_chunk_match",
            side_effect=fake_match,
        ):
            result = link_comparison.build_composite_datalinking_elements([
                link("AB", "A", "B"),
                link("BC", "B", "C"),
                link("AC", "A", "C"),
            ])

        self.assertEqual(result["construction"], "recursive_green_parent_hierarchy")
        self.assertEqual(len(result["composites"]), 1)
        merge_evidence = result["composites"][0]["mergeEvidence"]
        self.assertEqual(merge_evidence["chunk_evidence_strategy"], "cached_base_pair_metrics")
        self.assertEqual(merge_evidence["evaluated_profile_pair_count"], 3)
        self.assertEqual(merge_evidence["chunk_similarity"], 0.0)

    def test_only_green_parents_can_create_a_next_level(self):
        def link(link_id, left, right, score):
            return {
                "@id": link_id,
                "dataprofile1ref": left,
                "dataprofile2ref": right,
                "dp1Name": left,
                "dp2Name": right,
                "metrics": {"combined_similarity": score * 100},
                "common_keywords": "math",
                "description_top_chunks": [],
                "_description_chunks_by_profile": {
                    left: {"id": left, "name": left, "chunks": [f"{left}-chunk"]},
                    right: {"id": right, "name": right, "chunks": [f"{right}-chunk"]},
                },
            }

        with patch.object(link_comparison, "_profile_pair_original_chunk_match", return_value=None):
            result = link_comparison.build_composite_datalinking_elements([
                link("AB", "A", "B", 0.2),
                link("AC", "A", "C", 0.8),
            ])

        self.assertEqual(result["levels"][0]["items"][0]["status"], "red")
        self.assertEqual(result["levels"][0]["items"][1]["status"], "green")
        self.assertEqual(len(result["levels"]), 1)
        self.assertEqual(result["composites"], [])

    def test_red_child_is_terminal_final_composite(self):
        def link(link_id, left, right):
            return {
                "@id": link_id,
                "dataprofile1ref": left,
                "dataprofile2ref": right,
                "dp1Name": left,
                "dp2Name": right,
                "metrics": {"combined_similarity": 80},
                "common_keywords": "math",
                "description_top_chunks": [],
                "_description_chunks_by_profile": {
                    left: {"id": left, "name": left, "chunks": [f"{left}-chunk"]},
                    right: {"id": right, "name": right, "chunks": [f"{right}-chunk"]},
                },
            }

        with patch.object(
            link_comparison,
            "_profile_pair_original_chunk_match",
            return_value=None,
        ):
            result = link_comparison.build_composite_datalinking_elements([
                link("AB", "A", "B"),
                link("AC", "A", "C"),
                link("BC", "B", "C"),
            ])

        self.assertEqual(len(result["levels"]), 2)
        child = result["levels"][1]["items"][0]
        self.assertEqual(child["status"], "red")
        self.assertEqual(len(result["composites"]), 1)
        self.assertEqual(result["composites"][0]["profiles"], child["profiles"])

    def test_only_terminal_quartet_is_returned_as_final_composite(self):
        def link(left, right):
            return {
                "@id": f"{left}{right}",
                "dataprofile1ref": left,
                "dataprofile2ref": right,
                "dp1Name": left,
                "dp2Name": right,
                "metrics": {
                    "combined_similarity": 80,
                    "description_similarity": 80,
                    "keywords_similarity": 80,
                },
                "common_keywords": "math",
                "description_top_chunks": [],
            }

        result = link_comparison.build_composite_datalinking_elements([
            link("A", "B"),
            link("A", "C"),
            link("A", "D"),
            link("B", "C"),
            link("B", "D"),
            link("C", "D"),
        ])

        self.assertEqual(len(result["levels"]), 3)
        self.assertEqual(len(result["composites"]), 1)
        self.assertEqual(
            {profile["id"] for profile in result["composites"][0]["profiles"]},
            {"A", "B", "C", "D"},
        )


if __name__ == "__main__":
    unittest.main()
