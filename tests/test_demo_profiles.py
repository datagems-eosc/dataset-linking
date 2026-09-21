import json
import unittest
from pathlib import Path

from dl.similarity import _is_profile_json


class DemoProfileTests(unittest.TestCase):
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
