import os
from functions.get_bearer_access_token import (get_bearer_access_token, encode_api_credentials)

# API key and secret are saved as environment variables
secret = os.getenv("IDEALISTA_SECRET")
api_key = os.getenv("IDEALIST_API_KEY")

# get bearer access token
base64_authorization_string = encode_api_credentials(api_key=api_key, secret=secret)
BAT = get_bearer_access_token(base64_authorization_string=base64_authorization_string)
