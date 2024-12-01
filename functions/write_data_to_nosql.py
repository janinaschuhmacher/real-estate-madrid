import pandas as pd
import os
import boto3
from botocore.exceptions import ClientError
import logging


class DynamoDB_Helper:
    def __init__(self):
        self.dynamodb_client = boto3.client("dynamodb")
        self.dynamodb_resource = boto3.resource("dynamodb")

        # set up logging to be printed to the console
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        # Create a logger
        logger = logging.getLogger()
        # create a consoe handler and set its log levl
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        # Add the console handler to the logger
        logger.addHandler(console_handler)

    def check_if_table_exists_NoSQL(
        self,
        table_name: str,
    ) -> bool:
        """Function that checks if a table already exists in the NoSQL database.

        Args:
            table_name (str): Name of the table that we look for in the NoSQL database.

        Returns:
            bool: True if table already exists, False otherwise.
        """
        try:
            self.dynamodb_client.describe_table(TableName=table_name)
            logging.info("Table %s already exists.", table_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                logging.info("Table %s does not exist.", table_name)
            else:
                logging.info("Unexpected error: %s", e)
            return False

    def create_table_NoSQL(self, table_name: str = "IdealistaDataMadrid") -> bool:
        """Function that creates a new table in the NoSQL database.

        Args:
            table_name (str, optional): Name of the table that we want to write to the database. Defaults to "IdealistaDataMadrid".

        Returns:
            bool: _description_
        """
        # check if a table with name table_name already exists in the database
        if self.check_if_table_exists_NoSQL(table_name=table_name):
            logging.info("Table %s already exists", table_name)
            return None

        # Create table
        try:
            self.dynamodb_client.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "insert_date", "KeyType": "HASH"},
                    {"AttributeName": "run", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "insert_date", "AttributeType": "S"},
                    {"AttributeName": "run", "AttributeType": "N"},
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            )
            logging.info("Table %s created successfully.", table_name)
            return True
        except ClientError as e:
            logging.error("Failed to create table %s: %e", table_name, e)
            return False

    def write_data_to_NoSQL(self, df: pd.DataFrame):
        """Function that writes data from the idealista API to a table in aws dynamoDB.

        Args:
            df (pd.DataFrame): data that is written to the database
        """
        # check that AWS credentials are set as environment variables
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION:
            logging.info("AWS credentials are set.")
        else:
            raise NameError(
                "AWS credentials are not fully set. Please ensure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_DEFAULT_REGION are properly configured."
            )

        # convert all columns to type string because dynamoDB cannot handle floats
        df_write = df.copy(deep=True)
        df_write.reset_index(inplace=True)
        df_write = df_write.astype("str")

        # set the sort key
        df_write["run"] = df_write.index

        # write each row as one record to the database
        records = df_write.to_dict(orient="records")
        table = self.dynamodb_resource.Table("IdealistaDataMadrid")
        success_count = 0
        error_count = 0

        with table.batch_writer() as batch:
            for record in records:
                try:
                    batch.put_item(Item=record)
                    success_count += 1
                except ClientError as e:
                    print("Failed to write record to DynamoDB: e", e)
                    error_count += 1

        print(f"Total records written: {success_count}")
        print(f"Total records failed: {error_count}")

        return df_write
