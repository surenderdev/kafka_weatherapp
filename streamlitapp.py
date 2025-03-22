import streamlit as st
import psycopg2
import pandas as pd
#from dotenv import load_dotenv
import os
import logging
import pandas as pd


# Load environment variables
#load_dotenv()

'''
# PostgreSQL Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "weather_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

'''
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "weather_db"
DB_USER ="postgres"
DB_PASSWORD = "password"

def connect_db():
    """Connects to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logging.info("Connected to PostgreSQL database.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        raise
'''
# Function to Fetch and Display All Weather Data
def fetch_all_weather_data(conn):
    """Fetches all data from the weather_data table and displays it."""
    try:
        with conn.cursor() as cursor:
            # Retrieve all data from the table
            cursor.execute("SELECT * FROM weather_data ORDER BY id ASC")
            rows = cursor.fetchall()
            
            # Check if there is any data in the table
            if rows:
                print("\nDetailed Weather Data:")
                for row in rows:
                    print(f"ID: {row[0]}, City: {row[1]}, Date: {row[2]}, Time: {row[3]}, "
                          f"Temperature: {row[4]}°C, Humidity: {row[5]}%, Weather: {row[6]}, "
                          f"Latitude: {row[7]}, Longitude: {row[8]}")
            else:
                print("No weather data found in the database.")
    except Exception as e:
        logging.error(f"Error fetching weather data: {e}")
        raise
'''

def fetch_weather_data():
    """Fetch weather data from the database and return as Pandas DataFrame."""
    try:
        conn = connect_db()
        cursor = conn.cursor()
        query = """
            SELECT id, city_name, temperature, humidity, weather_description, latitude, longitude, date, time
            FROM weather_data
            ORDER BY id ASC;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        return df
    except Exception as e:
        logging.error(f"Failed to fetch weather data: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )

    logging.info("Starting to fetch weather data...")
    try:
        # Fetch and display all weather data
        df=fetch_weather_data()
        print(df)
        logging.info("Database connection closed.")

    except Exception as e:
        logging.error(f"Failed to fetch weather data: {e}")
