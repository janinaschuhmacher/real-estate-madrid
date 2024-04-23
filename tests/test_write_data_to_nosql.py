import unittest
from unittest.mock import patch, MagicMock, call
from functions.write_data_to_nosql import DynamoDB_Helper

import botocore.session
import botocore.errorfactory
from botocore.exceptions import ClientError

import pandas as pd
import os


class TestDynamoDBHelper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # patch connection to DynamoDB
        cls.patcher = patch("functions.write_data_to_nosql.boto3.client")
        cls.mock_dynamodb_client = cls.patcher.start()
        cls.mock_dynamodb = MagicMock()
        cls.mock_dynamodb_client.return_value = cls.mock_dynamodb
        # set up DynamoDB client
        cls.dynamodb_helper = DynamoDB_Helper()

    @classmethod
    def tearDownClass(cls):
        cls.patcher.stop()
        super().tearDownClass()

    @patch("functions.write_data_to_nosql.logging")
    def test_table_does_not_exist(self, mock_logging):
        """Test case in which the table does not exist in the database."""
        # set up ResourceNotFoundException
        model = botocore.session.get_session().get_service_model("dynamodb")
        factory = botocore.errorfactory.ClientExceptionsFactory()
        exceptions = factory.create_client_exceptions(model)

        self.mock_dynamodb.exceptions.ResourceNotFoundException = (
            exceptions.ResourceNotFoundException
        )

        # mock so that describe_table returns ResourceNotFoundException
        self.mock_dynamodb.describe_table.side_effect = exceptions.ResourceNotFoundException(
            error_response={
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "Requested resource not found: Table: test_table not found",
                }
            },
            operation_name="DescribeTable",
        )

        # run _check_if_table_exists_NoSQL()
        test_table_name = "test_table"
        result = self.dynamodb_helper._check_if_table_exists_NoSQL(test_table_name)

        # check that function returns False
        self.assertRaises(self.mock_dynamodb.exceptions.ResourceNotFoundException)
        mock_logging.info.assert_called_with(
            "Table %s does not exist.", test_table_name
        )
        self.assertFalse(result)

    @patch("functions.write_data_to_nosql.logging")
    def test_table_already_exists(self, mock_logging):
        """Test case in which the table already exists in the database."""
        test_table_name = "test_table"
        self.mock_dynamodb.describe_table.side_effect = None

        # call _check_if_table_exists_NoSQL
        result = self.dynamodb_helper._check_if_table_exists_NoSQL(test_table_name)

        # check that function returns True
        mock_logging.info.assert_called_with(
            "Table %s already exists.", test_table_name
        )
        self.assertTrue(result)


class TestCreateTableNoSQL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # patch connection to DynamoDB
        cls.patcher = patch("functions.write_data_to_nosql.boto3.client")
        cls.mock_dynamodb_client = cls.patcher.start()
        cls.mock_dynamodb = MagicMock()
        cls.mock_dynamodb_client.return_value = cls.mock_dynamodb
        # set up DynamoDB client
        cls.dynamodb_helper = DynamoDB_Helper()

    @classmethod
    def tearDownClass(cls):
        cls.patcher.stop()
        super().tearDownClass()

    @patch("functions.write_data_to_nosql.logging")
    def test_table_creation_success(self, mock_logging):
        """Test successful table creation"""
        # mock _check_if_table_exists_NoSQL function and return False
        with patch.object(
            DynamoDB_Helper, "_check_if_table_exists_NoSQL", return_value=False
        ):
            # Call the function create_table_NoSQL
            test_table_name = "test_table"
            result = self.dynamodb_helper.create_table_NoSQL(test_table_name)

            # Assert that the table creation was successful
            DynamoDB_Helper._check_if_table_exists_NoSQL.assert_called_once()
            self.mock_dynamodb.create_table.assert_called_once()
            mock_logging.info.assert_called_with(
                "Table %s created successfully.", test_table_name
            )
            self.assertTrue(result)

    @patch("functions.write_data_to_nosql.logging")
    def test_table_creation_already_existing(self, mock_logging):
        """Test that function exists if the table already exists"""
        # mock _check_if_table_exists_NoSQL function and return False
        with patch.object(
            DynamoDB_Helper, "_check_if_table_exists_NoSQL", return_value=True
        ):
            # Call the function create_table_NoSQL
            test_table_name = "test_table"
            result = self.dynamodb_helper.create_table_NoSQL(test_table_name)

            # Assert that the table creation was successful
            DynamoDB_Helper._check_if_table_exists_NoSQL.assert_called_once()
            assert not self.mock_dynamodb.called
            mock_logging.info.assert_called_with(
                "Table %s already exists", test_table_name
            )
            self.assertIsNone(result)


class TestWriteToDynamoDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dynamodb_helper = DynamoDB_Helper()

        # provide sample df
        cls.df = pd.DataFrame(
            columns=[
                "propertyCode",
                "thumbnail",
                "externalReference",
                "numPhotos",
                "floor",
                "price",
                "propertyType",
                "operation",
                "size",
                "exterior",
                "rooms",
                "bathrooms",
                "address",
                "province",
                "municipality",
                "district",
                "country",
                "neighborhood",
                "latitude",
                "longitude",
                "showAddress",
                "url",
                "distance",
                "description",
                "hasVideo",
                "status",
                "newDevelopment",
                "hasLift",
                "parkingSpace",
                "priceByArea",
                "detailedType",
                "suggestedTexts",
                "hasPlan",
                "has3DTour",
                "has360",
                "hasStaging",
                "topNewDevelopment",
                "superTopHighlight",
                "labels",
                "highlight",
            ],
            data=[
                [
                    "123456678",
                    "https://img3.idealista.com/blur/WEB_LISTING/0/",
                    "",
                    9,
                    1,
                    1500.0,
                    "flat",
                    "rent",
                    70.0,
                    True,
                    1,
                    1,
                    "Calle Argumosa",
                    "Madrid",
                    "Madrid",
                    "Centro",
                    "es",
                    "Lavapiés-Embajadores",
                    40.410107,
                    -3.696234,
                    False,
                    "https://www.idealista.com/inmueble/123456678/",
                    969,
                    "¡PRECIOSO apartamento EN PLENO CORAZÓN DE MADR...",
                    False,
                    "good",
                    False,
                    True,
                    "",
                    21.0,
                    "{'typology': 'flat'}",
                    "{'subtitle': 'Lavapiés-Embajadores, Madrid'}",
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    "",
                    "",
                ],
            ],
        )

    @classmethod
    def tearDownClass(cls):
        # cls.patcher.stop()
        super().tearDownClass()

    @patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "your_access_key",
            "AWS_SECRET_ACCESS_KEY": "your_secret_key",
            "AWS_DEFAULT_REGION": "your_region",
        },
    )
    # @patch("functions.write_data_to_nosql.os.getenv", return_value=True)
    @patch("functions.write_data_to_nosql.boto3")
    def test_write_data_to_NoSQL_success(self, mock_boto3):
        # mock dataframe
        test_df = pd.DataFrame(
            columns=[
                "index",
                "propertyCode",
                "thumbnail",
                "externalReference",
                "numPhotos",
                "floor",
                "price",
                "propertyType",
                "operation",
                "size",
                "exterior",
                "rooms",
                "bathrooms",
                "address",
                "province",
                "municipality",
                "district",
                "country",
                "neighborhood",
                "latitude",
                "longitude",
                "showAddress",
                "url",
                "distance",
                "description",
                "hasVideo",
                "status",
                "newDevelopment",
                "hasLift",
                "parkingSpace",
                "priceByArea",
                "detailedType",
                "suggestedTexts",
                "hasPlan",
                "has3DTour",
                "has360",
                "hasStaging",
                "topNewDevelopment",
                "superTopHighlight",
                "labels",
                "highlight",
                "run",
            ],
            data=[
                [
                    "0",
                    "123456678",
                    "https://img3.idealista.com/blur/WEB_LISTING/0/",
                    "",
                    "9",
                    "1",
                    "1500.0",
                    "flat",
                    "rent",
                    "70.0",
                    "True",
                    "1",
                    "1",
                    "Calle Argumosa",
                    "Madrid",
                    "Madrid",
                    "Centro",
                    "es",
                    "Lavapiés-Embajadores",
                    "40.410107",
                    "-3.696234",
                    "False",
                    "https://www.idealista.com/inmueble/123456678/",
                    "969",
                    "¡PRECIOSO apartamento EN PLENO CORAZÓN DE MADR...",
                    "False",
                    "good",
                    "False",
                    "True",
                    "",
                    "21.0",
                    "{'typology': 'flat'}",
                    "{'subtitle': 'Lavapiés-Embajadores, Madrid'}",
                    "False",
                    "False",
                    "False",
                    "False",
                    "False",
                    "False",
                    "",
                    "",
                    0,
                ],
            ],
        )

        # Mocking DynamoDB batch_writer
        mock_batch_writer = MagicMock()
        mock_batch_writer.__enter__.return_value = mock_batch_writer
        mock_batch_writer.__exit__.return_value = False

        # Mocking DynamoDB Table
        mock_table = MagicMock()
        mock_table.batch_writer.return_value = mock_batch_writer
        mock_table.put_item.side_effect = [
            None,
            None,
            None,
        ]  # Successful write

        # Mocking DynamoDB client and its operations
        mock_dynamodb = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_boto3.client.return_value = mock_dynamodb

        # # Instantiate YourClass
        obj = DynamoDB_Helper()

        # Call the function
        result = obj.write_data_to_NoSQL(self.df)

        # Assertions
        mock_boto3.client.assert_called_once_with("dynamodb")
        mock_dynamodb.Table.assert_called_once_with("IdealistaDataMadrid")
        self.assertEqual(mock_batch_writer.put_item.call_count, 1)
        pd.testing.assert_frame_equal(
            result, test_df
        )  # Check if DataFrame is modified as expected

    # test case where keys are not set
    @patch("functions.write_data_to_nosql.os.getenv", return_value=False)
    def test_write_data_to_NoSQL_missing_access_key(self, mock_getenv):
        obj = DynamoDB_Helper()

        with self.assertRaises(NameError):
            obj.write_data_to_NoSQL(self.df)


if __name__ == "__main__":
    unittest.main()
