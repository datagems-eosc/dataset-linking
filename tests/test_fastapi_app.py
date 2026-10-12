import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from dl.fastapi_app import app


class FastAPICompositeEndpointTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    @patch("dl.fastapi_app.compare_datalinkingbase_links", return_value=[])
    @patch("dl.fastapi_app.build_croissant_report", return_value={"links": []})
    @patch("dl.fastapi_app.compute_similarities", return_value=(None, [], False))
    def test_compare_links_endpoint(self, compute, report, compare):
        response = self.client.post(
            "/api/compare-links",
            json={"folder": "demo_profiles", "relation_th": 0.5, "force_refresh": True},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["@type"], "DataLinkingBaseComparisonReport")
        compute.assert_called_once()
        self.assertTrue(compute.call_args.kwargs["force_refresh"])
        compare.assert_called_once()

    @patch(
        "dl.fastapi_app.build_composite_datalinking_elements",
        return_value={
            "levels": [],
            "recursive_levels": [],
            "connected_component_levels": [],
            "composites": [],
            "final_composites": [],
            "connected_component_composites": [],
            "comparisons": [],
            "recursive_comparisons": [],
        },
    )
    @patch("dl.fastapi_app._load_profiles_for_composites", return_value=({}, {}))
    @patch("dl.fastapi_app.build_croissant_report", return_value={"links": []})
    @patch("dl.fastapi_app.compute_similarities", return_value=(None, [], False))
    def test_composites_endpoint(self, compute, report, profiles, builder):
        response = self.client.get(
            "/api/composites",
            params={"folder": "demo_profiles", "relation_th": 0.5, "force_refresh": "true"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["@type"], "CompositeDataLinkingElementReport")
        compute.assert_called_once()
        self.assertTrue(compute.call_args.kwargs["force_refresh"])
        profiles.assert_called_once_with("demo_profiles")
        builder.assert_called_once()
        self.assertTrue(builder.call_args.kwargs["force_refresh"])


if __name__ == "__main__":
    unittest.main()
