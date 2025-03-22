# 🌤️ Real-Time Weather App

## Overview
A Python-based Streamlit web application for fetching and displaying real-time weather data. This app integrates Kafka for data streaming, PostgreSQL for database management, and Streamlit for an interactive UI. Users can input a city name to retrieve weather details, which are dynamically displayed in the app.

---

## Features
- **Real-Time Weather Updates**: Fetches live weather data for cities entered by the user.
- **Dynamic UI**: Displays the latest weather data in real-time with automatic updates.
- **Kafka Integration**: Employs Kafka producer and consumer for asynchronous data processing.
- **PostgreSQL Database**: Stores and retrieves weather data using a structured relational database.
- **Custom Styling**: Includes a semi-transparent background and a styled DataFrame display.
- **Multithreaded Processing**: Runs background tasks for data fetching, processing, and UI updates seamlessly.
- **Streamlined Experience**: Removes Streamlit's default menu and footer for a focused user interface.

---

## Technologies Used
- **Python**: Programming language for backend logic.
- **Streamlit**: Web application framework for building the UI.
- **Confluent Kafka**: Data streaming platform for producer-consumer communication.
- **PostgreSQL**: Database system for storing weather data.
- **Pandas**: For data manipulation and analysis.
- **psycopg2**: PostgreSQL database adapter for Python.

---
