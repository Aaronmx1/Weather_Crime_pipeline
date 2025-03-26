# Weather_pipeline
Pipeline which retrieves weather data from open-meteo and loads ingested data into MariaDB

# Overview
3/11/2025 iteration: This is the initial phase of my data pipeline setup which consists of one table storing weather data from open-meteo's API.  The cities queried were Los Angeles, Orange County, and San Diego from the dates 1/1/2024 to 1/1/2025.  There were roughly 26k records retrieved and stored which I plan to build upon in order to generate a more complex schema that allows for additional analysis.

3/26/2025 iteration: Reworked my crime API to ingest data and store raw data into bronze layer along with meta-data.  Updated my transfer from bronze to silver layer, but need to add additional cleaning methods before transfer to silver layer.  Updated the Schema to incorporate bronze and silver layers.  Updated Crimes table to reflect schema, need to rework the Weather table in next iteration.

# Schema
<img src="blob:chrome-untrusted://media-app/3fc1853d-ee5f-4e49-bfbf-1261eb068b0d" />![image](https://github.com/user-attachments/assets/82da83d9-6d80-446d-b8d4-949879063c5a)

