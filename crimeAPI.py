"""
@author: Aaron
Overview: Retrieve crime data through data.gov API
API Endpoint: https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8/about_data
"""

#pip install pandas
#pip install sodapy

import pandas as pd
import os
import json  # parse json data into python dictionary
from sodapy import Socrata
from insertData import insert_crime_data_bronze_layer, insert_crime_data_silver_layer
from db_connector import get_crime_weather_db_connection  # import database connection operator

def crimeData_API_transfer_to_bronze_layer():
    ''' Connect to Crime data API, retrieve data, and store into database. '''

    # Authenticated client to perform API call
    client = Socrata("data.lacity.org",
                    os.getenv('APP_TOKEN'),
                    username=os.getenv('CRIMEUSERNAME'),
                    password=os.getenv('CRIMEPASSWORD'))
    
    counter = 0
    increment = 50000  # Quantity of data to pull per API call
    while(counter < 1100000):
        try:
            api = "2nrs-mtv8"  # API link to data

            # Retrieve results in 50k record increments
            # Paging results: https://dev.socrata.com/docs/paging.html
            results = client.get(api, limit=increment, offset=counter)

            # Store json payload and meta data for insertion into database
            crimeDict = {}
            crimeDict["jsonData"] = results
            crimeDict["last_rowNumber"] = counter+increment  # maintains last value pulled during API call
            crimeDict["api"] = api

            # Insert into database
            insert_crime_data_bronze_layer(crimeDict)
            
            # Increment offset
            counter += increment

        except ValueError:
            print("failed")
            exit(1)

def transfer_crimeData_bronze_to_silver_layer():
    ''' Connect to databases bronze layer to retrieve raw data, then parse JSON data into fields, and store in silver layer table. '''
    # Retrieve database connection
    conn = get_crime_weather_db_connection()

    if conn is not None:
        try:
            # Get cursor for interacting with bronze layer
            cur = conn.cursor()

            # Retrieve records from database and store into dataframe
            cur.execute("SELECT json_data FROM b_Crimes")
            b_dataDF = pd.DataFrame(cur)  # Convert cursor object into Datafame object for easier iterating.

            # Fields matching up to database insertion
            expectedFields = ["dr_no", "date_rptd", "date_occ", "time_occ", "area", "area_name", "rpt_dist_no",
                "part_1_2","crm_cd", "crm_cd_desc", "mocodes", "vict_age", "vict_sex", "vict_descent", "premis_cd",
                "premis_desc","weapon_used_cd","weapon_desc","status","status_desc","crm_cd_1",
                "crm_cd_2","crm_cd_3","crm_cd_4","location","cross_street","lat","lon"]

            crimeArray = []  # Stores database records            
            for idx, row in b_dataDF.iterrows():
                json_string = row[0]  # Extract the JSON string
                try:
                    data = json.loads(json_string)  # Decode JSON string to list of dictionary objects

                    for record in data:
                        crimeDict = {}  # Stores individual records
                        for field in expectedFields:
                            crimeDict[field] = record.get(field, None)  # get() retrieves value based on key and returns None if not found
                        crimeArray.append(crimeDict)
                except json.JSONDecodeError:
                    print(f"Error decoding JSON: {json_string}")
            
            # Convert array of dictionary fields into dataframe records for insertion into database
            results_df = pd.DataFrame.from_records(crimeArray)

            # Insert data into database
            insert_crime_data_silver_layer(results_df)

            # Successful termination
            print("Successfully loaded.")        


        except Exception as e:
            print(f"An error occurred: {e}")
            exit(1)


if __name__ == '__main__':
    crimeData_API_transfer_to_bronze_layer()
    #transfer_crimeData_bronze_to_silver_layer()