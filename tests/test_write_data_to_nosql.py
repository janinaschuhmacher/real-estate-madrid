import unittest
from unittest.mock import patch, MagicMock

from functions.write_data_to_nosql import DynamoDB_Helper

import botocore.session
import botocore.errorfactory

import pandas as pd
import os
from global_variables import TEST_DATA_DIRECTORY


class TestCreateTableNoSQL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # patch connection to DynamoDB
        cls.patcher_client = patch("functions.write_data_to_nosql.boto3.client")
        cls.patcher_resource = patch("functions.write_data_to_nosql.boto3.resource")
        cls.mock_dynamodb_client = cls.patcher_client.start()
        cls.mock_dynamodb_resource = cls.patcher_resource.start()
        cls.mock_dynamodb = MagicMock()
        cls.mock_dynamodb_client.return_value = cls.mock_dynamodb
        cls.mock_dynamodb_resource.return_value = cls.mock_dynamodb

        # set up DynamoDB client
        cls.dynamodb_helper = DynamoDB_Helper()

    @classmethod
    def tearDownClass(cls):
        cls.patcher_client.stop()
        cls.patcher_resource.stop()
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

        # run check_if_table_exists_NoSQL()
        test_table_name = "test_table"
        result = self.dynamodb_helper.check_if_table_exists_NoSQL(test_table_name)

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

        # call check_if_table_exists_NoSQL
        result = self.dynamodb_helper.check_if_table_exists_NoSQL(test_table_name)

        # check that function returns True
        mock_logging.info.assert_called_with(
            "Table %s already exists.", test_table_name
        )
        self.assertTrue(result)

    @patch("functions.write_data_to_nosql.logging")
    def test_table_creation_success(self, mock_logging):
        """Test successful table creation"""
        # mock check_if_table_exists_NoSQL function and return False
        with patch.object(
            DynamoDB_Helper,
            "check_if_table_exists_NoSQL",
            return_value=False,
            autospec=True,
        ):
            # Call the function create_table_NoSQL
            test_table_name = "test_table"
            result = self.dynamodb_helper.create_table_NoSQL(test_table_name)

            # Assert that the table creation was successful
            self.dynamodb_helper.check_if_table_exists_NoSQL.assert_called_once()  # pylint: disable=E1101
            self.mock_dynamodb.create_table.assert_called_once()
            mock_logging.info.assert_called_with(
                "Table %s created successfully.", test_table_name
            )
            self.assertTrue(result)

    @patch("functions.write_data_to_nosql.logging")
    def test_table_creation_already_existing(self, mock_logging):
        """Test that function exists if the table already exists"""
        # mock check_if_table_exists_NoSQL function and return False
        with patch.object(
            DynamoDB_Helper,
            "check_if_table_exists_NoSQL",
            return_value=True,
            autospec=True,
        ):
            # Call the function create_table_NoSQL
            test_table_name = "test_table"
            result = self.dynamodb_helper.create_table_NoSQL(test_table_name)

            # Assert that the table creation was successful
            self.dynamodb_helper.check_if_table_exists_NoSQL.assert_called_once()  # pylint: disable=E1101
            assert not self.mock_dynamodb.called
            mock_logging.info.assert_called_with(
                "Table %s already exists", test_table_name
            )
            self.assertIsNone(result)


class TestWriteToDynamoDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # provide sample df
        cls.data_directory = TEST_DATA_DIRECTORY
        cls.file_name_idealista_data = "df_nosql_test.csv"
        cls.file_name_idealista_data_processed = "df_nosql_test_result.pkl"
        cls.file_path_idealista_data = os.path.join(
            cls.data_directory, cls.file_name_idealista_data
        )
        cls.file_path_idealista_data_processed = os.path.join(
            cls.data_directory, cls.file_name_idealista_data_processed
        )
        cls.df = pd.read_csv(cls.file_path_idealista_data, keep_default_na=False)
        cls.test_df = pd.read_pickle(cls.file_path_idealista_data_processed)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    @patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "your_access_key",
            "AWS_SECRET_ACCESS_KEY": "your_secret_key",
            "AWS_DEFAULT_REGION": "your_region",
        },
    )
    @patch("functions.write_data_to_nosql.boto3")
    def test_write_data_to_NoSQL_success(self, mock_boto3):
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
        mock_boto3.resource.return_value = mock_dynamodb

        # Instantiate YourClass
        obj = DynamoDB_Helper()

        # Call the function
        result = obj.write_data_to_NoSQL(self.df)

        # Assertions
        mock_boto3.client.assert_called_once_with("dynamodb")
        mock_dynamodb.Table.assert_called_once_with("IdealistaDataMadrid")
        self.assertEqual(mock_batch_writer.put_item.call_count, 1)
        pd.testing.assert_frame_equal(
            result, self.test_df
        )  # Check if DataFrame is modified as expected

    # test case where keys are not set
    @patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_DEFAULT_REGION": "",
        },
    )
    # @patch("functions.write_data_to_nosql.boto3")
    def test_write_data_to_NoSQL_missing_access_key(self):
        with patch("functions.write_data_to_nosql.boto3"):
            with self.assertRaises(NameError):
                obj = DynamoDB_Helper()
                obj.write_data_to_NoSQL(self.df)


if __name__ == "__main__":
    unittest.main()
