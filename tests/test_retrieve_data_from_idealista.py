from real_estate_madrid.functions.retrieve_data_from_idealista import (
    retrieve_data_from_idealista,
    url_encode_request_data,
)
import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal
import os

from real_estate_madrid.utils.global_variables import TEST_DATA_DIRECTORY


class TestUrlEcodeRequestData(unittest.TestCase):
    def test_url_encode_request_data(self):
        """TestUrlEcodeRequestData 1: Test function url_encode_request_data"""
        data = (
            "locale=es&operation=rent&propertyType=homes&center=40.416944%2C-3.703333&distance=5000&"
            + "hasMultimedia=True&preservation=good&maxItems=50&minPrice=200&minSize=40&"
            + "sinceDate=W&order=ratioeurm2&sort=asc&furnished=furnishedKitchen"
        )
        test_data = url_encode_request_data(furnished="furnishedKitchen")
        self.assertEqual(test_data, data)


class TestRetrieveDataFromIdealista(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_directory = TEST_DATA_DIRECTORY
        cls.file_name_idealista_data_raw = "idealista_data_short.txt"
        cls.file_name_idealista_data_transformed = "df_idealista_data_short.pkl"
        cls.file_path_idealista_data_raw = os.path.join(
            cls.data_directory, cls.file_name_idealista_data_raw
        )
        cls.file_path_idealista_data_transformed = os.path.join(
            cls.data_directory, cls.file_name_idealista_data_transformed
        )
        with open(cls.file_path_idealista_data_raw, "r") as file:
            cls.idealista_data_raw = file.read()

        cls.idealista_data = pd.read_pickle(cls.file_path_idealista_data_transformed)
        cls.mock_post_patcher = patch(
            "real_estate_madrid.functions.retrieve_data_from_idealista.requests"
        )
        cls.mock_post = cls.mock_post_patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.mock_post_patcher.stop()

    def test_retrieve_data_from_idealista(self):
        """TestRetrieveDataFromIdealista 1: Test function retrieve_data_from_idealista"""
        self.mock_post.post().status_code = 200
        self.mock_post.post().text = str(self.idealista_data_raw)

        test_response, totalPages, actualPage = retrieve_data_from_idealista(
            request_data="locale=es&operation=rent&propertyType=homes&locationId=0-EU-ES-28",
            access_token="bbbbb",
        )

        assert totalPages == 1
        assert actualPage == 1
        assert_frame_equal(test_response, self.idealista_data)
