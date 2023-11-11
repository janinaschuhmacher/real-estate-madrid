import os
import pandas as pd
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

# API key and secret are saved as environment variables
secret = os.getenv("IDEALISTA_SECRET")
api_key = os.getenv("IDEALIST_API_KEY")

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
        df, totalPages, actualPage, _ = retrieve_data_from_idealista(
            request_data=request_data, access_token=BAT
        )

        actualPage += 1
        df_all = pd.concat([df_all, df], ignore_index=True)

        # write unit tests
        # write documentation

# write data to csv for analysis
backup_idealista_data(file_name="idealista_data.csv")
append_idealista_data(file_name="idealista_data.csv", df_new_data=df_all)
remove_duplicates_from_csv("idealista_data.csv")
