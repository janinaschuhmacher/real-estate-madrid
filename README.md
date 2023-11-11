# Explore real estate data for Madrid using idealista

The goal of this project is to analyze rental prices in Madrid across different neighborhoods, as well as to visualize price developments over time. 
The data is taken from the idealista API (access can be requested here: https://developers.idealista.com/access-request (link last used on 04.11.2023)).

![Picture of roofs in Madrid](Madrid_Foto.jpeg)
*Picture retrieved on 05.11.2023 from https://pixabay.com/de/photos/madrid-spanien-sonnenuntergang-2714570/ by Stan89, license: https://pixabay.com/de/service/license-summary/*


## About the API

Filters that I always apply are:
* country=es
* operation=rent
* propertyType=homes
* center=40.416944,-3.703333 (for Madrid city center)
* distance=5000 (for a radius of 5 km aroud the center)
* hasMultimedia=True (meaning property has pictures, a video or a virtual tour)
* preservation=good
* maxItems (items per page, max. 50)
* numPage (page number, we iterate through the pages)
* maxPrice / minPrice (maximum and minimum rent)
* sinceDate (W:last week, M: last month, T:last day (for rent except rooms), Y: last 2 days (sale and rooms))
* order (distance, price, street, photos, publicationDate, modificationDate, size, floor, rooms, ratioeurm2)
* sort (asc or desc)
* minSize / maxSize (from 60 m2 to 1000m2)
* bedrooms (0,1,2,3,4: , bedroom number separated by commas. examples: "0", "1,4","0,3", "0,2,4". 4 means "4 or more")


Other relevant filters include:
* furnished (furnished or furnishedKitchen if there is only a kitchen)

Filters that are currently not used are:
* locationId=0-EU-ES-28 (for Madrid, Spain)
* airConditioning (boolean)

