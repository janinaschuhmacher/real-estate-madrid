import pybase64
from requests.structures import CaseInsensitiveDict
import requests
import ast
from datetime import timedelta


def get_bearer_access_token(
    base64_authorization_string: str,
) -> str:
    """
    Function that retrieves the current bearer access token using
    the idealista API key and secret.
    This is needed to retrieve any information from the API.

    :param base64_authorization_string: encoded authorization header

    :return: Bearer access token.
    """
    url = "https://api.idealista.com/oauth/token"

    headers = CaseInsensitiveDict()
    headers["Authorization"] = base64_authorization_string
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    headers["Accept"] = "application/json"
    data = "grant_type=client_credentials"

    # retrieve bearer access token
    response = requests.post(url, headers=headers, data=data, timeout=5)
    assert (
        response.status_code == 200
    ), """get_bearer_access_token: API Call failed with status code {status_code}
            and message {text}.
            No bearer access token was retrieved.""".format(
        status_code=response.status_code, text=response.text
    )

    dict_resp = ast.literal_eval(response.text)
    access_token = dict_resp["access_token"]
    assert (
        len(access_token) > 0
    ), "get_bearer_access_token: No bearer access token was retrieved."

    expiration_secs = dict_resp["expires_in"]
    print(
        "Bearer access token expires in:",
        timedelta(seconds=expiration_secs),
        "(hh:mm:ss)",
    )

    return access_token


def encode_api_credentials(
    secret: str,
    api_key: str,
) -> str:
    """
    Function that reads the idealista api key and secret and encodes it as specified in the documentation:
    (1) URL encode your API key and secret according to RFC 1738
    (2) Concatenate the encoded API key, a colon character ":", and the secret into a single string
    (3) Base64 encode the string from the previous step

    :param secret: idealista api secret
    :param api_key: idealista api key

    :return: encoded authorization header
    """
    # URL encode your API key and secret according to RFC 1738
    authorization_string = "{api_key}:{secret}".format(secret=secret, api_key=api_key)
    # Base64 encode the string
    base64_authorization_string = pybase64.b64encode(
        bytes(authorization_string, "utf-8")
    )
    base64_authorization_string = "Basic " + str(base64_authorization_string)[2:-1]

    return base64_authorization_string
