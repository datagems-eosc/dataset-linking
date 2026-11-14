import os
import unittest
from unittest.mock import Mock, patch

from dl import datagems_auth


class DataGEMSAuthenticationTests(unittest.TestCase):
    def setUp(self):
        datagems_auth.reset_access_token_cache()

    def tearDown(self):
        datagems_auth.reset_access_token_cache()

    def test_credentials_are_required(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(datagems_auth.DataGEMSAuthenticationError):
                datagems_auth.get_access_token()

    @patch("dl.datagems_auth.requests.post")
    def test_client_credentials_token_is_cached_until_expiry(self, post):
        response = Mock()
        response.json.return_value = {"access_token": "token-one", "expires_in": 300}
        response.raise_for_status.return_value = None
        post.return_value = response

        environment = {
            "OIDC_CLIENT_ID": "dataset-linking",
            "OIDC_CLIENT_SECRET": "test-secret",
        }
        with patch.dict(os.environ, environment, clear=True), patch(
                "dl.datagems_auth.time.monotonic", return_value=100.0
        ):
            first = datagems_auth.get_access_token()
            second = datagems_auth.get_access_token()

        self.assertEqual(first, "token-one")
        self.assertEqual(second, "token-one")
        post.assert_called_once()
        self.assertEqual(post.call_args.kwargs["data"], {
            "grant_type": "client_credentials",
            "client_id": "dataset-linking",
            "client_secret": "test-secret",
            "scope": "datagems",
        })

    @patch("dl.datagems_auth.requests.post")
    def test_force_refresh_obtains_a_new_token(self, post):
        first_response = Mock()
        first_response.json.return_value = {"access_token": "token-one", "expires_in": 300}
        first_response.raise_for_status.return_value = None
        second_response = Mock()
        second_response.json.return_value = {"access_token": "token-two", "expires_in": 300}
        second_response.raise_for_status.return_value = None
        post.side_effect = [first_response, second_response]

        environment = {
            "OIDC_CLIENT_ID": "dataset-linking",
            "OIDC_CLIENT_SECRET": "test-secret",
        }
        with patch.dict(os.environ, environment, clear=True):
            self.assertEqual(datagems_auth.get_access_token(), "token-one")
            self.assertEqual(datagems_auth.get_access_token(force_refresh=True), "token-two")

        self.assertEqual(post.call_count, 2)

    @patch("dl.datagems_auth.requests.post")
    def test_expired_token_is_renewed_automatically(self, post):
        first_response = Mock()
        first_response.json.return_value = {"access_token": "token-one", "expires_in": 60}
        first_response.raise_for_status.return_value = None
        second_response = Mock()
        second_response.json.return_value = {"access_token": "token-two", "expires_in": 60}
        second_response.raise_for_status.return_value = None
        post.side_effect = [first_response, second_response]

        environment = {
            "OIDC_CLIENT_ID": "dataset-linking",
            "OIDC_CLIENT_SECRET": "test-secret",
        }
        with patch.dict(os.environ, environment, clear=True), patch(
                "dl.datagems_auth.time.monotonic", side_effect=[100.0, 131.0]
        ):
            self.assertEqual(datagems_auth.get_access_token(), "token-one")
            self.assertEqual(datagems_auth.get_access_token(), "token-two")

        self.assertEqual(post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
