import unittest
from unittest.mock import patch, MagicMock
from functions.write_data_to_nosql import DynamoDB_Helper

import botocore.session
import botocore.errorfactory


class TestDynamoDBHelper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.patcher = patch("functions.write_data_to_nosql.boto3.client")
        cls.mock_dynamodb_client = cls.patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.patcher.stop()
        super().tearDownClass()

    def test_table_exists(self):
        # set up DynamoDB client
        self.mock_dynamodb = MagicMock()
        self.mock_dynamodb_client.return_value = self.mock_dynamodb
        self.dynamodb_helper = DynamoDB_Helper()

        # set up ResourceNotFoundError
        model = botocore.session.get_session().get_service_model("dynamodb")
        factory = botocore.errorfactory.ClientExceptionsFactory()
        exceptions = factory.create_client_exceptions(model)

        # set up ResourceNotFoundException
        self.mock_dynamodb.exceptions.ResourceNotFoundException = (
            exceptions.ResourceNotFoundException
        )

        test_table_name = "test_table"

        self.mock_dynamodb.describe_table.side_effect = exceptions.ResourceNotFoundException(
            error_response={
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "Requested resource not found: Table: test_table not found",
                }
            },
            operation_name="DescribeTable",
        )

        result = self.dynamodb_helper._check_if_table_exists_NoSQL(test_table_name)

        self.assertRaises(self.mock_dynamodb.exceptions.ResourceNotFoundException)
        self.assertFalse(result)


#     @patch("functions.write_data_to_nosql.boto3.client")
#     def test_table_does_not_exist(self, mock_dynamodb_client):
#         mock_dynamodb = MagicMock()
#         mock_dynamodb_client.return_value = mock_dynamodb

#         # set up DynamoDB client
#         dynamodb_helper = DynamoDB_Helper()

#         test_table_name = "test_table"

#         mock_dynamodb.describe_table.side_effect = None

#         result = dynamodb_helper._check_if_table_exists_NoSQL(test_table_name)

#         self.assertTrue(result)


# class TestCreateTableNoSQL(unittest.TestCase):
#     @patch("functions.write_data_to_nosql.boto3.client")
#     def test_table_creation_success(self, mock_dynamodb_client):
#         mock_dynamodb = MagicMock()
#         mock_dynamodb_client.return_value = mock_dynamodb

#         # set up DynamoDB client
#         dynamodb_helper = DynamoDB_Helper()

#         # mock _check_if_table_exists_NoSQL function and return False
#         DynamoDB_Helper._check_if_table_exists_NoSQL = MagicMock(return_value=False)

#         # Call the function
#         test_table_name = "test_table"
#         result = dynamodb_helper.create_table_NoSQL(test_table_name)

#         # Assert that the table creation was successful
#         DynamoDB_Helper._check_if_table_exists_NoSQL.assert_called_once()
#         mock_dynamodb.create_table.assert_called_once()
#         self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
