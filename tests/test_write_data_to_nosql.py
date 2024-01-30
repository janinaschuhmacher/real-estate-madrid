import unittest
from unittest.mock import patch, MagicMock
from functions.write_data_to_nosql import (
    DynamoDBHelper,
)
import boto3
import botocore.session
import botocore.errorfactory


class TestDynamoDBHelper(unittest.TestCase):
    @patch("boto3.client")
    def test_table_exists(self, mock_dynamodb_client):
        mock_dynamodb = MagicMock()

        mock_dynamodb_client.return_value = mock_dynamodb

        model = botocore.session.get_session().get_service_model("dynamodb")
        factory = botocore.errorfactory.ClientExceptionsFactory()
        exceptions = factory.create_client_exceptions(model)

        # set up ResourceNotFoundException
        mock_dynamodb.exceptions.ResourceNotFoundException = (
            exceptions.ResourceNotFoundException
        )

        # Set up DynamoDB Helper
        dynamodb_helper = DynamoDBHelper()
        test_table_name = "test_table"

        mock_dynamodb.describe_table.side_effect = exceptions.ResourceNotFoundException(
            error_response={
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "Requested resource not found: Table: test_table not found",
                }
            },
            operation_name="DescribeTable",
        )

        result = dynamodb_helper._check_if_table_exists_NoSQL(test_table_name)

        self.assertRaises(mock_dynamodb.exceptions.ResourceNotFoundException)
        self.assertFalse(result)


# class TestCheckIfTableExistsNoSQL(unittest.TestCase):
#     @patch("functions.write_data_to_nosql.boto3.client")
#     def test_table_does_not_exist(self, mock_boto3_client):
#         model = botocore.session.get_session().get_service_model("dynamodb")
#         factory = botocore.errorfactory.ClientExceptionsFactory()
#         exceptions = factory.create_client_exceptions(model)

#         # Assuming you have setup a mock boto3 client called `boto3_mock`
#         mock_boto3_client.exceptions.UserNotFoundException = (
#             exceptions.ResourceNotFoundException
#         )
#         # Create a mock DynamoDB client
#         mock_dynamodb_client = MagicMock()

#         mock_dynamodb_client.describe_table.side_effect = exceptions.ResourceNotFoundException(
#             error_response={
#                 "Error": {
#                     "Code": "ResourceNotFoundException",
#                     "Message": "Requested resource not found: Table: test_table not found",
#                 }
#             },
#             operation_name="DescribeTable",
#         )

#         # Set the return value of describe_table to raise ResourceNotFoundException
#         # def my_side_effect(TableName):
#         #     raise mock_boto3_client.exceptions.ResourceNotFoundException(
#         #         {}, "ResourceNotFoundException"
#         #     )

#         # mock_dynamodb_client.describe_table.side_effect = my_side_effect

#         mock_boto3_client.return_value = mock_dynamodb_client

#         print(mock_dynamodb_client.describe_table.side_effect)

#         # Call the function with a non-existent table name
#         result = _check_if_table_exists_NoSQL("test_table")

#         # Assert that the function returns False
#         self.assertFalse(result)


# class TestCreateTableNoSQL(unittest.TestCase):
#     @patch("boto3.client")
#     @patch("boto3.resource")
#     def test_table_creation_success(self, mock_resource, mock_client):
#         # Mock the DynamoDB client and resource
#         mock_client_instance = mock_client.return_value
#         mock_resource_instance = mock_resource.return_value

#         # Simulate table not existing initially
#         mock_client_instance.describe_table.side_effect = [
#             mock_client_instance.exceptions.ResourceNotFoundException(
#                 {}, "ResourceNotFoundException"
#             ),
#             None,  # Table doesn't exist after creation attempt
#         ]

#         # Simulate successful table creation
#         mock_resource_instance.create_table.return_value = None

#         # Call the function
#         result = create_table_NoSQL("test_table")

#         # Assert that the table creation was successful
#         self.assertTrue(result)
#         mock_resource_instance.create_table.assert_called_once()

#     @patch("boto3.client")
#     def test_table_already_exists(self, mock_client):
#         # Mock the DynamoDB client
#         mock_client_instance = mock_client.return_value

#         # Simulate table already existing
#         mock_client_instance.describe_table.side_effect = None

#         # Call the function
#         result = create_table_NoSQL("test_table")

#         # Assert that the function returns False indicating table already exists
#         self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
