import os
import base64
import streamlit as st
import pandas as pd
import threading
import time
import psycopg2
from dotenv import load_dotenv
from producer import produce_weather_data
from consumer_db import consume_weather_data
import logging
load_dotenv()

# Global configuration
timeinterval = 30
topicname = "weatherdata_topic"
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Shared resources
stop_event = threading.Event()
data_lock = threading.Lock()  # Lock for thread-safe access to shared data
latest_data = None  # Shared variable to store the latest fetched database data

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# Background image setup
@st.cache_resource
def get_base64_of_bin_file(bin_file):
    """Converts binary file to base64 string."""
    try:
        with open(bin_file, 'rb') as f:
            data = f.read()
        return base64.b64encode(data).decode()
    except Exception as e:
        st.error(f"Error reading file: {e}")
        return None

def set_png_as_page_bg(png_file):
    """Sets PNG file as the persistent background for the page."""
    bin_str = get_base64_of_bin_file(png_file)
    if bin_str:
        page_bg_img = f"""
        <style>
        body {{
            background-image: url("data:image/png;base64,{bin_str}") !important;
            background-size: cover !important;
            background-position: center !important;
            background-repeat: no-repeat !important;
            background-attachment: fixed !important;
        }}
        .stApp {{
            background: transparent !important;
            background-color: rgba(255, 255, 255, 0.8);  !important; /* Semi-transparent white */
            box-shadow: none !important;
        }}
        </style>
        """
        st.markdown(page_bg_img, unsafe_allow_html=True)
    else:
        st.warning("Background image not set. Please check the file path.")



# Database connection and fetching
def connect_db():
    """Connect to the PostgreSQL database."""
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


def fetch_weather_data():
    """Fetch weather data from the database and return as Pandas DataFrame."""
    try:
        conn = connect_db()
        cursor = conn.cursor()
        query = """
            SELECT city_name, temperature, humidity, weather_description, latitude, longitude, date, time
            FROM weather_data
            ORDER BY date DESC, time DESC;
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


# Kafka producer thread
def start_producer():
    """Run the producer."""
    while not stop_event.is_set():
        produce_weather_data(city_name, API_KEY, timeinterval, topicname)
        time.sleep(timeinterval)


# Kafka consumer thread
def start_consumer():
    """Run the consumer."""
    while not stop_event.is_set():
        consume_weather_data(topicname)
        time.sleep(5)


# Background thread to periodically fetch data
def periodic_data_fetch():
    """Fetch weather data periodically and update the shared data variable."""
    global latest_data
    while not stop_event.is_set():
        try:
            df = fetch_weather_data()
            with data_lock:
                latest_data = df  # Safely update the shared variable
        except Exception as e:
            logging.error(f"Error fetching weather data: {e}")
        time.sleep(5)  # Fetch data every 10 seconds

def display_weather_data(container):
    """Display the latest fetched weather data in Streamlit."""
    global latest_data
    with data_lock:
        if latest_data is not None and not latest_data.empty:
            logging.info(f"Displaying DataFrame with {len(latest_data)} records.")
            container.write("### Weather Data from Database")
            container.dataframe(latest_data)
        else:
            logging.warning("No weather data available to display.")
            container.warning("No weather data available yet. Please wait.")


# Background thread manager
def start_threads():
    """Start producer, consumer, and periodic database fetching threads."""
    producer_thread = threading.Thread(target=start_producer, daemon=True)
    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    db_thread = threading.Thread(target=periodic_data_fetch, daemon=True)

    producer_thread.start()
    consumer_thread.start()
    db_thread.start()

    return producer_thread, consumer_thread, db_thread


# Main Streamlit application
def main():
    """Main function for the Streamlit app."""
    global API_KEY
    API_KEY = os.getenv("API_KEY")
    if not API_KEY:
        st.error("API key not found.")
        return
    st.set_page_config(page_title="Weather App", layout="wide")
    # Add custom CSS to hide the top menu bar and Streamlit footer
   # Add custom CSS to hide the top menu bar and Streamlit footer
    hide_menu_style = """
        <style>
        /* Hides the top menu bar */
        #MainMenu {display: none;}  /* Completely removes the hamburger menu */
        header {visibility: hidden;}  /* Hides the Streamlit top header */
        footer {visibility: hidden;}  /* Hides the Streamlit footer */
        </style>
        """
    st.markdown(hide_menu_style, unsafe_allow_html=True)

    set_png_as_page_bg("asset/backgroundimage1.png")
    st.title("🌤️ Real-Time Weather App")
    global city_name
    city_name = st.text_input("City Name", placeholder="Enter a city name")

    # Create a placeholder for dynamic data display
    data_container = st.empty()

    if st.button("Submit"):
        if not city_name:
            st.warning("Please enter a valid city name.")
        else:
            st.success(f"Fetching data for {city_name}...")
            producer_thread, consumer_thread, db_thread = start_threads()

            # Main loop to keep refreshing the displayed data
            try:
                while not stop_event.is_set():
                    display_weather_data(data_container)  # Update the placeholder area
                    time.sleep(5)  # Refresh the UI every second
            except KeyboardInterrupt:
                stop_event.set()
                producer_thread.join()
                consumer_thread.join()
                db_thread.join()
                st.info("All threads stopped.")


if __name__ == "__main__":
    main()
