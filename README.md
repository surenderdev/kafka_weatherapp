# 🌤️ Real-Time Weather App

## Overview
A Python-based Streamlit web application for fetching and displaying real-time weather data. This app integrates Kafka for data streaming, PostgreSQL for database management, and Streamlit for an interactive UI. Users can input a city name to retrieve weather details, which are dynamically displayed in the app.

In this app, multithreading is implemented to perform multiple background tasks without blocking the main application.

### Threads in Action:

### Producer Thread:

Fetches weather data from an external weather API for the user-specified city.

Continuously sends weather data to a Kafka topic (weatherdata_topic).

### Consumer Thread:

Listens to the Kafka topic for incoming weather data.

Validates and stores data in the PostgreSQL database.

Ensures new weather information is always added to the database.

### Database Fetching Thread:

Periodically retrieves the latest weather data from the database.

Updates a shared variable (latest_data) used to refresh the displayed content on the Streamlit UI.

### Thread Management:

All threads operate independently, leveraging Python's threading module.

Controlled using a global stop_event to gracefully shut down tasks when the app exits.

---

## Features
- **Real-Time Weather Updates**: Fetches live weather data for cities entered by the user.
- **Dynamic UI**: Displays the latest weather data in real-time with automatic updates.
- **Kafka Integration**: Employs Kafka producer and consumer for asynchronous data processing.
- **PostgreSQL Database**: Stores and retrieves weather data using a structured relational database.
- **Multithreaded Processing**: Runs background tasks for data fetching, processing, and UI updates seamlessly.
- **Streamlined Experience**: Customizes Streamlit's defaults for a focused user interface.

---

## Technologies Used
#Python #Streamlit #ConfluentKafka #PostgreSQL #Pandas #psycopg2
---
