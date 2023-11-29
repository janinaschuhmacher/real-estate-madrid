from functions.get_bearer_access_token import (
    get_bearer_access_token,
    encode_api_credentials,
)
from unittest.mock import patch
from nose.tools import assert_equal
import unittest


class TestEncodeApiCredentials(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.api_key = "abc"
        cls.secret = "123"

    def test_encode_api_credentials(self):
        """TestEncodeApiCredentials 1: Test function encode_api_credentials"""
        test_base64_authorization_string = encode_api_credentials(
            api_key=self.api_key, secret=self.secret
        )
        assert_equal(test_base64_authorization_string, "Basic YWJjOjEyMw==")


class TestGetBearerAccessToken(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mock_post_patcher = patch("functions.get_bearer_access_token.requests")
        cls.mock_post = cls.mock_post_patcher.start()

        cls.mock_encode_api_credentials_patcher = patch(
            "functions.get_bearer_access_token.encode_api_credentials"
        )
        cls.mock_encode_api_credentials = (
            cls.mock_encode_api_credentials_patcher.start()
        )

    @classmethod
    def tearDownClass(cls):
        cls.mock_post_patcher.stop()
        cls.mock_encode_api_credentials_patcher.stop()

    def test_get_bearer_access_token(self):
        """TestGetBearerAccessToken 1: Test function get_bearer_access_token"""
        self.mock_post.post().status_code = 200
        self.mock_post.post().text = "{'access_token': 'bbbbb', 'expires_in': 3600}"

        test_response = get_bearer_access_token(
            base64_authorization_string="authorization_string"
        )

        assert_equal(test_response, "bbbbb")
