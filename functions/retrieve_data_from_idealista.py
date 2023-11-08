import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict
import urllib
import json


def retrieve_data_from_idealista(
    request_data: str,
    access_token: str,
) -> pd.DataFrame:
    # build the complete request
    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Bearer " + access_token
    url_properties = "https://api.idealista.com/3.5/es/search?"

    response = requests.post(url_properties + request_data, headers=headers)

    assert (
        response.status_code == 200
    ), """retrieve_data_from_idealista: API Call failed with status code {status_code}
            and message {text}
            No data was retrieved.
        """.format(
        status_code=response.status_code, text=response.text
    )
    print(response.text)
    data = json.loads(response.text)
    data = pd.DataFrame.from_dict(data["elementList"])

    return data


def url_encode_request_data(
    locale: str = "es",
    operation: str = "rent",
    propertyType: str = "homes",
    locationId: str = "0-EU-ES-28",
    hasMultimedia: str = "True",
    preservation: str = "good",
    maxItems: str = "50",
    maxPrice: str = "2000",
    minPrice: str = "200",
    minSize: str = "40",
    sinceDate: str = "W",
    order: str = "ratioeurm2",
    sort: str = "asc",
    bedrooms: str = None,
    furnished: str = None,
    airConditioning: str = None,
    numPage: str = None,
) -> str:
    # set query parameters
    data_dict = {
        "locale": locale,
        "operation": operation,
        "propertyType": propertyType,
        "locationId": locationId,
        "hasMultimedia": hasMultimedia,
        "preservation": preservation,
        "maxItems": maxItems,
        "minPrice": minPrice,
        "minSize": minSize,
        "sinceDate": sinceDate,
        "order": order,
        "sort": sort,
        "bedrooms": bedrooms,
        "furnished": furnished,
        "airConditioning": airConditioning,
        "numPage": numPage,
    }

    # remove parameters without value
    for parameter in ["bedrooms", "furnished", "airConditioning", "numPage"]:
        if data_dict[parameter] is None:
            data_dict.pop(parameter)

    # url encode parameters
    data = urllib.parse.urlencode(data_dict)

    return data
