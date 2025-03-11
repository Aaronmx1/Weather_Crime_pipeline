"""
@author Aaron
Overview: Insert data into local Mariadb instance
"""

# import packages
import pandas as pd
import mariadb
from db_connector import get_db_connection  # import database connection operator

def insert_db_data(dataset):

    # Retrieve database connection
    conn = get_db_connection()

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