import unittest
from unittest.mock import patch

from dl import flask_app


def _similarity_pair():
    return {
        "dataprofile1": "Dataset A",
        "dataprofile2": "Dataset B",
        "id1": "dataset-a",
        "id2": "dataset-b",
        "keywords_similarity": 75.0,
        "description_similarity": 80.0,
        "headline_similarity": 70.0,
        "combined_similarity": 76.5,
        "common_keywords": "climate, temperature",
        "unique_to_1": "weather",
        "unique_to_2": "forecast",
        "description_top_chunks": [{"chunk1": "A", "chunk2": "B", "similarity": 80.0}],
    }


class SaveSingleTests(unittest.TestCase):
    def setUp(self):
        flask_app.ANALYSIS_CACHE.clear()
        self.client = flask_app.app.test_client()
        self.query = {
            "d1": "Dataset A",
            "d2": "Dataset B",
            "folder": "",
            "kw": "0.6",
            "desc": "0.3",
            "head": "0.1",
            "th": "30",
            "kw_method": "jaccard",
        }

    def tearDown(self):
        flask_app.ANALYSIS_CACHE.clear()

    def test_save_single_reuses_analysis_already_shown(self):
        cache_key = flask_app._analysis_cache_key("", 0.6, 0.3, 0.1, 30.0, "jaccard")
        flask_app._remember_analysis(cache_key, [_similarity_pair()], True)

        with patch.object(
                flask_app,
                "compute_similarities",
                side_effect=AssertionError("save must not recalculate the displayed analysis"),
        ):
            response = self.client.get("/save_single", query_string=self.query)

        self.assertEqual(response.status_code, 200)
        self.assertIn("downloadComplete=1", response.headers.get("Set-Cookie", ""))
        response.close()

    def test_save_single_fallback_uses_home_cache_configuration(self):
        with patch.object(
                flask_app,
                "compute_similarities",
                return_value=(None, [_similarity_pair()], True),
        ) as compute:
            response = self.client.get("/save_single", query_string=self.query)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(compute.call_args.kwargs["include_description_chunks"])
        response.close()


if __name__ == "__main__":
    unittest.main()
