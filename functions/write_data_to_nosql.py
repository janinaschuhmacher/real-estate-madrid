import pandas as pd
import os
import boto3
import botocore
from botocore.exceptions import ClientError
import logging


class DynamoDB_Helper:
    def __init__(self):
        self.dynamodb = boto3.client("dynamodb")

    def _check_if_table_exists_NoSQL(
        self,
        table_name: str,
    ) -> bool:
        try:
            self.dynamodb.describe_table(TableName=table_name)
            logging.info("Table '%s' already exists.", table_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                logging.info("Table '%s' does not exist.", table_name)
            else:
                logging.info("Unexpected error: %s" % e)
            return False

    def create_table_NoSQL(self, table_name: str = "IdealistaDataMadrid") -> bool:
        if self._check_if_table_exists_NoSQL(table_name=table_name):
            print("Table already exists", table_name)
            return

        # Create table
        try:
            self.dynamodb.create_table(
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
            logging.error("Failed to create table %s: %s", table_name, e)
            return False


def write_data_to_NoSQL(df: pd.DataFrame):
    """Function that writes data from the idealista API to a table in aws dynamoDB"""
    # check that AWS credentials are set as environment variables
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION:
        print("AWS credentials are set.")
    else:
        print(
            "AWS credentials are not fully set. Please ensure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_DEFAULT_REGION are properly configured."
        )

    # convert all columns to type string because dynamoDB cannot handle floats
    df.reset_index(inplace=True)
    df = df.astype("str")

    # set the sort key
    df["run"] = df.index

    # write each row as one record to the database
    dynamodb = boto3.client("dynamodb")
    records = df.to_dict(orient="records")
    table = dynamodb.Table("IdealistaDataMadrid")
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
