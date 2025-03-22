# 🌤️ Real-Time Weather App

## Overview
A Python-based Streamlit web application for fetching and displaying real-time weather data. This app integrates Kafka for data streaming, PostgreSQL for database management, and Streamlit for an interactive UI. Users can input a city name to retrieve weather details, which are dynamically displayed in the app.

In this app, multithreading is implemented to perform multiple background tasks without blocking the main application.
---
## Features
- **Real-Time Weather Updates**: Fetches live weather data for cities entered by the user.
- **Dynamic UI**: Displays the latest weather data in real-time with automatic updates.
- **Kafka Integration**: Employs Kafka producer and consumer for asynchronous data processing.
- **PostgreSQL Database**: Stores and retrieves weather data using a structured relational database.
- **Multithreaded Processing**: Runs background tasks for data fetching, processing, and UI updates seamlessly.
- **Streamlined Experience**: Customizes Streamlit's defaults for a focused user interface.

---
# Real-Time Weather App: Workflow

## **1. Input from User Interface (UI)**
- **Source**: Streamlit-based web application.
- **Process**: 
  - Users provide a **city name** via a text input field.
  - After clicking **"Submit"**, the app triggers backend threads to fetch, process, and display weather data for the city.
- **Output**:
  - Dynamic UI that refreshes to show the latest weather data retrieved from the database.
---
## **2. Source of Data**
- **API Used**: OpenWeather API.
- **Details Provided**:
  - **City Name**: User-entered location.
  - **Temperature**: Current temperature in Celsius.
  - **Humidity**: Percentage of atmospheric humidity.
  - **Weather Description**: A short description, e.g., "Cloudy" or "Sunny."
  - **Geolocation**: Latitude and longitude of the city.
  - **Timestamp**: Date and time for record-keeping.

---

## **3. Kafka Producer**
- **Role**: Fetches weather data from the API and sends it to a Kafka topic (`weatherdata_topic`).
- **Steps**:
  1. Fetches data using the city name and API key.
  2. Serializes the data to JSON for compatibility with Kafka.
  3. Sends the data to the Kafka topic.
  4. Logs successful or failed message deliveries.
- **Concurrency**: Runs in a separate thread to ensure non-blocking operations.

---

## **4. Kafka Consumer**
- **Role**: Reads messages from the Kafka topic and inserts weather data into the PostgreSQL database.
- **Steps**:
  1. Continuously listens to the `weatherdata_topic`.
  2. Processes each message to ensure it's valid weather data.
  3. Inserts new records into the database, avoiding duplicates by using unique constraints (`city_name`, `date`, `time`).
- **Concurrency**: Operates in its own thread for continuous processing.

---

## **5. PostgreSQL Database**
- **Purpose**: Provides persistent storage for weather data.
- **Schema**:
  - **Table Name**: `weather_data`
  - **Columns**:
    - `city_name`: Name of the city.
    - `temperature`: Current temperature.
    - `humidity`: Atmospheric humidity percentage.
    - `weather_description`: A brief description of the weather.
    - `latitude`, `longitude`: Geographical coordinates.
    - `date`, `time`: The timestamp for the data entry.
  - **Unique Constraints**:
    - Prevents duplicate records based on `city_name`, `date`, and `time`.

---

## **6. Periodic Database Fetch**
- **Role**: Retrieves the latest weather data from the database for display on the UI.
- **Process**:
  1. Periodically runs every 10 seconds to query new records from the database.
  2. Updates a shared variable (`latest_data`) that serves as the data source for the UI.
  3. Uses threading with locks (`data_lock`) to ensure thread-safe updates.

---

## **7. UI Display and Updates**
- **Dynamic Updates**:
  - The UI uses a **placeholder** (`st.empty()`) to display weather data.
  - Only the latest weather records are shown, with older content dynamically replaced.
- **Styling**:
  - Clean and modern layout with a semi-transparent background.
  - The Streamlit top menu and footer are hidden for a focused experience.

## This workflow ensures a seamless pipeline from user input to real-time weather data visualization.
---
### Threads in Action:

### Producer Thread:

Continuously fetches and sends weather data to a Kafka topic (weatherdata_topic).

### Consumer Thread:

Listens to the Kafka topic for incoming weather data.

Validates and stores data in the PostgreSQL database.


### Database Fetching Thread:

Periodically retrieves the latest weather data from the database.

Updates a shared variable (latest_data) used to refresh the displayed content on the Streamlit UI.

### Thread Management:

All threads operate independently, leveraging Python's threading module.

Controlled using a global stop_event to gracefully shut down tasks when the app exits.

---
#Python #Streamlit #ConfluentKafka #PostgreSQL #Pandas #psycopg2 #Multi-threading
---

