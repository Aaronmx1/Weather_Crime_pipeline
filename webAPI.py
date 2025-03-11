"""
@author: Aaron
Overview: Retrieve data through open-meteo API
"""
## open-meteo pip install packages:
#pip install openmeteo-requests
#pip install requests-cache retry-requests numpy pandas

# Import packages
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import time
from insertData import insert_db_data  # insert data into database


def open_meteo_api():
    # Setup the Open-Meteo API client with cache and retry on error - code by openMeteo for API connection
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    coordinatesDict = {
        'Area': ['Los_Angeles', 'Orange_County', 'San_Diego'],
        'Lat': [34.052235, 33.787914, 32.715736],
        'Lon': [-118.243683, -117.853104, -117.161087]
    }

    # Iterate through latitude and longitude CSV
    coordinatesDF = pd.DataFrame(coordinatesDict)
    # Create dataframe of latitude and longitude data
    latLonDF = pd.DataFrame(data={'Area': coordinatesDF['Area'], 'Lat': coordinatesDF['Lat'], 'Lon': coordinatesDF['Lon']})
    print(latLonDF)

    for row in latLonDF.iterrows():
        # store area name, latitude, and longitude for us in API fetch
        area = row[1]['Area']
        lat = row[1]['Lat']
        lon = row[1]['Lon']
            
        # API parameters
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": "2024-01-01",
            "end_date": "2025-01-01",
                "hourly": ["temperature_2m", "relative_humidity_2m", "rain", "weather_code", "cloud_cover"],
                "temperature_unit": "fahrenheit",
                "wind_speed_unit": "mph",
                "precipitation_unit": "inch"
            }
        
        responses = openmeteo.weather_api(url, params=params)
        
        # Process first location.
        response = responses[0]
        
        print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
        print(f"Elevation {response.Elevation()} m asl")
        print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
        print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")
        
        
        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
        hourly_rain = hourly.Variables(2).ValuesAsNumpy()
        hourly_weather_code = hourly.Variables(3).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(4).ValuesAsNumpy()
        
        # Convert date column data
        hourly_data = {"date": pd.date_range(
                start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
                end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                freq = pd.Timedelta(seconds = hourly.Interval()),
                inclusive = "left"
            )}
        
        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["rain"] = hourly_rain
        hourly_data["weather_code"] = hourly_weather_code
        hourly_data["cloud_cover"] = hourly_cloud_cover
        
        # Store dataframe
        hourly_dataframe = pd.DataFrame(data = hourly_data)
        
        # Append Area name to dataframe
        hourly_dataframe['AreaName'] = area

        # insertion check
        insert_db_data(hourly_dataframe)
        
        # pause for API call
        time.sleep(2)
    
if __name__ == '__main__':
    open_meteo_api()