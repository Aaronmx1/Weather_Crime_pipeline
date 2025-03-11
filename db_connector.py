"""
@author: Aaron
Overview: Connect to Mariadb
Source: https://mariadb.com/resources/blog/how-to-connect-python-programs-to-mariadb/
"""

#pip install mariadb
#pip install python-dotenv   # accessing .env

# import packages
import mariadb  # connect to MariaDB server
import sys
import os  # access directory
from dotenv import load_dotenv  # access .env
import pandas as pd

def get_db_connection():
    # Obtain .env variables
    load_dotenv()  # Load environment variables from .env
    user = os.getenv("DB_USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    port_str = os.getenv("PORT")
    database = os.getenv("DATABASE")

    if user is None or password is None or host is None or port_str is None or database is None:
        print("Error: One or more environment variables are missing.")
        sys.exit(1)

    try:
        port = int(port_str)  # Convert port to integer
    except ValueError:
        print("Error: Invalid port number in .env file.")
        sys.exit(1)

    try:
        # Establish connection to database using - .connect()
        conn = mariadb.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )

    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    return conn