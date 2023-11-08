from functions.retrieve_data_from_idealista import (
    retrieve_data_from_idealista,
    url_encode_request_data,
)
import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal


class TestUrlEcodeRequestData(unittest.TestCase):
    def test_url_encode_request_data(self):
        data = (
            "locale=es&operation=rent&propertyType=homes&locationId=0-EU-ES-28&hasMultimedia=True&"
            + "preservation=good&maxItems=50&minPrice=200&minSize=40&sinceDate=W&order=ratioeurm2&sort"
            + "=asc&furnished=furnishedKitchen"
        )
        test_data = url_encode_request_data(furnished="furnishedKitchen")
        self.assertEqual(test_data, data)


class TestRetrieveDataFromIdealista(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mock_post_patcher = patch("functions.retrieve_data_from_idealista.requests")
        cls.mock_post = cls.mock_post_patcher.start()

        with open("tests/test_data/idealista_data_short.txt", "r") as file:
            cls.idealista_data_raw = file.read()

        cls.idealista_data = pd.read_pickle(
            "tests/test_data/df_idealista_data_short.pkl"
        )

    @classmethod
    def tearDownClass(cls):
        cls.mock_post_patcher.stop()

    def test_retrieve_data_from_idealista(self):
        self.mock_post.post().status_code = 200
        self.mock_post.post().text = str(self.idealista_data_raw)

        test_response = retrieve_data_from_idealista(
            request_data="locale=es&operation=rent&propertyType=homes&locationId=0-EU-ES-28",
            access_token="bbbbb",
        )

        assert_frame_equal(test_response, self.idealista_data)
