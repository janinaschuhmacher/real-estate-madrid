import unittest
from unittest.mock import patch, MagicMock
from functions.write_data_to_nosql import DynamoDB_Helper

import botocore.session
import botocore.errorfactory


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

    def test_table_exists(self):
        """Test case in which the table already exists in the database."""
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
        self.assertFalse(result)

    def test_table_does_not_exist(self):
        """Test case in which the table does not exist in the database."""

        test_table_name = "test_table"

        self.mock_dynamodb.describe_table.side_effect = None

        result = self.dynamodb_helper._check_if_table_exists_NoSQL(test_table_name)

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

    def test_table_creation_success(self):
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
            self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
