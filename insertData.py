"""
@author Aaron
Overview: Insert data into local Mariadb instance
"""

# import packages
import pandas as pd
import mariadb
from db_connector import get_weatherDB_connection, get_crime_weather_db_connection  # import database connection operator
from datetime import datetime
import json

def insert_weather_data(dataset):

    # Retrieve database connection
    conn = get_weatherDB_connection()

    # Check for active connection
    if conn is not None:
        try: 
            # Get Cursor - Cursor provides interface for interacting with the Server (ie. running SQL queries and managing transactions)
            cur = conn.cursor()

            # Iterate through dataset
            for idx, row in dataset.iterrows():
                date_str = row['date']
                temperature_2m = row['temperature_2m']
                relative_humidity_2m = row['relative_humidity_2m']
                rain = row['rain']
                weather_code = row['weather_code']
                cloud_cover = row['cloud_cover']
                Area_Name = row['AreaName']

                # Convert date string to datetime object
                date_obj = pd.to_datetime(date_str).strftime('%Y-%m-%d %H:%M:%S')

                # Insert into database
                cur.execute("INSERT INTO WeatherForecasts (date, temperature_2m, humidity_2m, rain, weather_code, cloud_cover, areaName) VALUES (?,?,?,?,?,?,?)",
                    (date_obj, temperature_2m, relative_humidity_2m, rain, weather_code, cloud_cover, Area_Name),
                    )
                
                # Commit insertion into database
                conn.commit()
                print(f"Last Inserted ID: {cur.lastrowid}")

            # Close connection to database - allows updates to be reflected immediately
            cur.close()
            conn.close()

        except mariadb.Error as e:
            print(f"Error: {e}")

    else:
        print("Connection not established.")

def insert_crime_data_bronze_layer(dataset):
    ''' Receives crime data from API, adds meta-data, and stores payload in raw form along with meta-data inside database. '''

    # Retrieve database connection
    conn = get_crime_weather_db_connection()

    if conn is not None:
        try:
            # Get cursor to provide interface for interacting with Server
            cur = conn.cursor()
            print("cur: ", cur)

            # Parse payload
            jsonData = dataset["jsonData"]
            lastRowNumber = dataset["last_rowNumber"]
            api = dataset["api"]

            jsonPayload = json.dumps(jsonData)  # Convert to JSON payload
            currentTime = datetime.now()        # Meta-data to track insertion into database

            # Insert records into database
            cur.execute("INSERT INTO b_Crimes (json_data, last_rowNumber, timeUploaded, api) VALUES (?,?,?,?)", (jsonPayload, lastRowNumber, currentTime, api))

            # Commit insertion into database
            conn.commit()
            print(f"Last Inserted ID: {cur.lastrowid}")

            # Close connection to database. Allows updates to be reflected immediately.
            cur.close()
            conn.close()

        except mariadb.Error as e:
            print(f"Error: {e}")
            return
    else:
        print("Connection not established.")

def insert_crime_data_silver_layer(dataset):
    ''' Receives Dataframe object and inserts into database. '''
    # Retrieve database connection
    conn = get_crime_weather_db_connection()

    if conn is not None:
        try:
            # Get Cursor - Cursor provides interface for interacting with the Server (ie. running SQL queries and managing transactions)
            cur = conn.cursor()

            # Insert records into database
            cur.executemany("INSERT INTO s_Crimes (dr_no, date_rptd, date_occ, time_occ, area, area_name, rpt_dist_no, part_1_2, crm_cd, crm_cd_desc, mocodes, vict_age, vict_sex, vict_descent, premis_cd, premis_desc, weapon_used_cd, weapon_desc, status, status_desc, crm_cd_1, crm_cd_2, crm_cd_3, crm_cd_4, location, cross_street, lat, lon) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dataset.values.tolist())

            # Commit insertion into database
            conn.commit()
            print(f"Last Inserted ID: {cur.lastrowid}")

            # Close connection to database - allows updates to be reflected immediately
            cur.close()
            conn.close()

        except mariadb.Error as e:
            print(f"Error: {e}")
            return
    else:
        print("Connection not established.")