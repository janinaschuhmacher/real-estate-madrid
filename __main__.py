"""Program that extracts data on rental apartments in Madrid from the idealista API"""

import os
import sys
from datetime import datetime
import pandas as pd

# get absolute path to project's root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# add the project's root directory to the Python path
sys.path.insert(0, project_root)

from functions.get_bearer_access_token import (
    get_bearer_access_token,
    encode_api_credentials,
)
from functions.retrieve_data_from_idealista import (
    url_encode_request_data,
    retrieve_data_from_idealista,
)

from functions.save_data_to_csv import (
    backup_idealista_data,
    append_idealista_data,
    remove_duplicates_from_csv,
)

from functions.write_data_to_nosql import DynamoDB_Helper

dynamodb_helper = DynamoDB_Helper()

# API key and secret are saved as environment variables
secret = os.getenv("IDEALISTA_SECRET")
api_key = os.getenv("IDEALISTA_API_KEY")

# get bearer access token
base64_authorization_string = encode_api_credentials(api_key=api_key, secret=secret)
BAT = get_bearer_access_token(base64_authorization_string=base64_authorization_string)

# retrieve data from last week from idealista
df_all = pd.DataFrame()
for furnished in ["furnishedKitchen", "furnished"]:
    totalPages = 20
    actualPage = 1

    while actualPage <= totalPages:
        request_data = url_encode_request_data(furnished=furnished, numPage=actualPage)
        df, totalPages, actualPage = retrieve_data_from_idealista(
            request_data=request_data, access_token=BAT
        )
        df["furnished"] = furnished
        df["insert_date"] = datetime.today().strftime("%Y-%m-%d")

        actualPage += 1
        df_all = pd.concat([df_all, df], ignore_index=True)

# write data to csv for analysis
backup_idealista_data(file_name="idealista_data.csv")
append_idealista_data(file_name="idealista_data.csv", df_new_data=df_all)
remove_duplicates_from_csv(file_name="idealista_data.csv")

# write data to NoSQL database
dynamodb_helper.create_table_NoSQL()
dynamodb_helper.write_data_to_NoSQL(df_all)
