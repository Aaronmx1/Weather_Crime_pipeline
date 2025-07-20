# Overview
This project stores two types of data, weather and crime records, to identify if there is a correlation between weather and crime statistics.  
The weather data is pulled through an API from open-meteo and crime records are CSV imports of Los Angeles crime pulled from data.gov.  
The emphasis of this project is to showcase the ELT process of ingesting data from two data sources (API & CSV), loading unclean raw data into the database, then transforming any unclean data into useable clean data and adding any necessary meta-data in order to query interesting insights.
Due to the nature of the crime dataset I will need to normalize and create relations between the tables, but my main purpose right now is to provide insights about what this dataset contains.

# Technology used
- Python for API and CSV ingestion and data cleaning
- SQL for creating and querying database tables
- MariaDb for storing data


# Schema
<img src="blob:chrome-untrusted://media-app/78fc49ef-6235-40b7-adb0-1be4acf07db9" />![image](https://github.com/user-attachments/assets/d4b32753-df69-44c4-b9df-59581f6c1572)


