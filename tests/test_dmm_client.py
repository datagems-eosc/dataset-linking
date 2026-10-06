import unittest
from unittest.mock import patch

import requests

from dl.dmm_client import (
    DMMRegistrationError,
    prepare_graph_for_dmm,
    register_graph_in_dmm,
)
from dl import fastapi_app


GRAPH = {
    "nodes": [
        {
            "id": "dataset-1",
            "labels": ["sc:Dataset"],
            "properties": {"name": "Dataset 1", "description": "private metadata"},
        },
        {
            "id": "comparison-1",
            "labels": ["PropertyComparison"],
            "properties": {"similarityScore": 80.0, "targetProperty": "keywords"},
        },
    ],
    "edges": [
        {
            "from": "comparison-1",
            "to": "dataset-1",
            "labels": ["HAS_TARGET"],
            "properties": {"similarityScore": 80.0},
        }
    ],
}


class DMMClientTests(unittest.TestCase):
    def test_prepare_graph_clears_only_dataset_properties(self):
        prepared = prepare_graph_for_dmm(GRAPH)

        self.assertEqual(prepared["nodes"][0]["properties"], {})
        self.assertEqual(
            prepared["nodes"][1]["properties"],
            GRAPH["nodes"][1]["properties"],
        )
        self.assertEqual(prepared["edges"], GRAPH["edges"])
        self.assertNotEqual(prepared, GRAPH)
        self.assertNotEqual(GRAPH["nodes"][0]["properties"], {})

    @patch("dl.dmm_client.requests.post")
    def test_register_sends_sanitized_graph_and_bearer_token(self, post):
        response = post.return_value
        response.status_code = 200
        response.json.return_value = {
            "code": 200,
            "message": "ok",
            "content": {},
        }

        result = register_graph_in_dmm(GRAPH, token="test-token")

        self.assertEqual(result["code"], 200)
        request = post.call_args.kwargs
        self.assertEqual(request["headers"]["Authorization"], "Bearer test-token")
        self.assertEqual(request["json"]["nodes"][0]["properties"], {})
        self.assertEqual(request["json"]["nodes"][1]["properties"], GRAPH["nodes"][1]["properties"])
        self.assertEqual(request["json"]["edges"], GRAPH["edges"])

    @patch("dl.dmm_client.requests.post")
    def test_non_2xx_response_is_reported(self, post):
        response = post.return_value
        response.status_code = 422
        response.text = "invalid graph"
        response.raise_for_status.side_effect = requests.HTTPError(response=response)

        with self.assertRaises(DMMRegistrationError):
            register_graph_in_dmm(GRAPH, token="test-token")

    @patch("dl.dmm_client.requests.post", side_effect=requests.Timeout())
    def test_timeout_is_reported(self, post):
        with self.assertRaises(DMMRegistrationError):
            register_graph_in_dmm(GRAPH, token="test-token")

    @patch("dl.fastapi_app.register_graph_in_dmm")
    @patch("dl.fastapi_app.get_access_token", return_value="test-token")
    @patch("dl.fastapi_app.refine_similarity", return_value={"graph": GRAPH})
    def test_generated_graph_is_registered(self, refine, get_token, register):
        summary = fastapi_app._attach_graphs_to_similarities(
            None,
            [{"passes_threshold": True, "id1": "one", "id2": "two"}],
            0.6,
            0.3,
            0.1,
            30.0,
        )

        self.assertEqual(summary["generated"], 1)
        refine.assert_called_once()
        get_token.assert_called_once()
        register.assert_called_once_with(GRAPH, token="test-token")

    @patch("dl.fastapi_app._attach_graphs_to_similarities")
    @patch("dl.fastapi_app.compute_similarities", return_value=("analysis failed", [], False))
    def test_failed_analysis_does_not_register_a_graph(self, compute, attach):
        job_id = "job-test"
        fastapi_app.JOBS[job_id] = {
            "params": {"normalized": True},
            "status": "queued",
        }
        try:
            fastapi_app._run_report_job(
                job_id,
                None,
                0.6,
                0.3,
                0.1,
                30.0,
                include_graphs=True,
            )
            self.assertEqual(fastapi_app.JOBS[job_id]["status"], "failed")
        finally:
            fastapi_app.JOBS.pop(job_id, None)

        compute.assert_called_once()
        attach.assert_not_called()


if __name__ == "__main__":
    unittest.main()
