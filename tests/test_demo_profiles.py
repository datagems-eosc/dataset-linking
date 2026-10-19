import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dl import similarity
from dl.similarity import _is_profile_json


class DemoProfileTests(unittest.TestCase):
    def test_api_cache_uses_configured_folder(self):
        with patch.dict(os.environ, {"DATA_LINKING_CACHE_DIR": "~/dataset-linking-cache"}):
            self.assertEqual(
                similarity.get_api_cache_folder(),
                Path("~/dataset-linking-cache").expanduser(),
            )

    def test_api_cache_uses_deploy_mount_when_available(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            deploy_cache = Path(temp_dir) / "s3" / "cache"
            deploy_cache.mkdir(parents=True)
            with patch.dict(os.environ, {}, clear=True), patch.object(
                    similarity, "DEPLOY_CACHE_FOLDER", deploy_cache
            ), patch.object(
                    similarity.sys, "platform", "linux"
            ):
                self.assertEqual(similarity.get_api_cache_folder(), deploy_cache)

    def test_api_cache_uses_local_folder_on_windows_even_if_deploy_path_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            deploy_cache = Path(temp_dir) / "s3" / "cache"
            deploy_cache.mkdir(parents=True)
            local_cache = Path(temp_dir) / "local-cache"
            with patch.dict(os.environ, {}, clear=True), patch.object(
                    similarity, "DEPLOY_CACHE_FOLDER", deploy_cache
            ), patch.object(
                    similarity, "LOCAL_CACHE_FOLDER", local_cache
            ), patch.object(similarity.sys, "platform", "win32"):
                self.assertEqual(similarity.get_api_cache_folder(), local_cache)

    def test_cache_json_is_not_a_profile(self):
        self.assertFalse(_is_profile_json(Path("cache_local_example.json")))
        self.assertTrue(_is_profile_json(Path("weather_observations.json")))

    def test_demo_profiles_are_small_valid_dataset_profiles(self):
        profiles = sorted(
            path for path in Path("demo_profiles").glob("*.json")
            if not path.name.startswith("cache_")
        )
        self.assertEqual(len(profiles), 10)

        ids = set()
        for path in profiles:
            data = json.loads(path.read_text(encoding="utf-8"))
            self.assertTrue(data["@id"])
            self.assertNotIn(data["@id"], ids)
            ids.add(data["@id"])
            self.assertTrue(data["name"])
            self.assertTrue(data["headline"])
            self.assertTrue(data["description"])
            self.assertGreaterEqual(len(data["keywords"]), 5)
            self.assertTrue(data["distribution"])
            self.assertTrue(data["recordSet"])


if __name__ == "__main__":
    unittest.main()
