import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict
import urllib
import json


def retrieve_data_from_idealista(
    request_data: str,
    access_token: str,
) -> pd.DataFrame:
    """
    Function that retrieves data from idealista API.
    Each call returns one page with a maximum of 50 listings per page.
    For example, if there were 150 listings that match our filters, we would have to
    make three requests for pages 1, 2, 3 with listings 1-50, 51-100 and 101-150, respectively.

    :param request_data: data that is passed to the POST request.
    :para access_token: bearer access token for idealista API.

    :return df: dataframe with the requested data from idealista.
    :return totalPages: total number of pages for the specified query parameters.
    :return actualPage: page number from which we requested the listings.
    """
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
    # print(response.text)
    data = json.loads(response.text)
    df = pd.DataFrame.from_dict(data["elementList"])
    df["filters"] = str(data["summary"])

    total = data["total"]
    totalPages = data["totalPages"]
    actualPage = data["actualPage"]

    print(
        total,
        "listings on idealista match the query parameters (page",
        actualPage,
        "out of",
        totalPages,
        ")",
    )

    return df, totalPages, actualPage


def url_encode_request_data(
    locale: str = "es",
    operation: str = "rent",
    propertyType: str = "homes",
    # locationId: str = "0-EU-ES-28",
    center: str = "40.416944,-3.703333",
    distance: str = "5000",
    hasMultimedia: str = "True",
    preservation: str = "good",
    maxItems: str = "50",
    maxPrice: str = None,
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
    """
    Function which takes the query parameters for our API request and formats them as url string
    that can be passed to the data parameter of the POST function.

    :param country: "es" for Spain.
    :param operation: "rent" meaning we look for rental.
    :param propertyType: "homes" meaning flats or houses.
    :param locationId: "0-EU-ES-28" for Madrid, Spain (can be used instead of center and distance).
    :param center: "40.416944,-3.703333" for Madrid city center.
    :param distance: "5000" for 5 km radius around the center.
    :param hasMultimedia: "True" (meaning property has pictures, a video or a virtual tour).
    :param preservation: "good" to exclude flats that need renovation.
    :param maxItems: items per page, max. 50.
    :param maxPrice: maximum rent.
    :params minPrice: minimum rent.
    :param minSize: minimum size of the flat.
    :param sinceDate: W:last week, M: last month, T:last day (for rent except rooms), Y: last 2 days (sale and rooms).
    :param order: results can be ordered by distance, price, street, photos, publicationDate,
                  modificationDate, size, floor, rooms, ratioeurm2.
    :param sort: "asc" to sort in ascending order.
    :param bedrooms: can 0,1,2,3,4: , bedroom number separated by commas.
                     examples: "0", "1,4","0,3", "0,2,4". 4 means "4 or more".
    :param furnished: "furnished" or "furnishedKitchen" if unfurnished except for the kitchen.
    :param airConditioning: "True" means that the flat has air conditioning.
    :param numPage: page number, we iterate through the pages.

    :return: url encoded string with query parameters.
    """
    # set query parameters
    data_dict = {
        "locale": locale,
        "operation": operation,
        "propertyType": propertyType,
        # "locationId": locationId,
        "center": center,
        "distance": distance,
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
