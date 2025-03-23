import json
import logging
import psycopg2
from psycopg2 import sql
from confluent_kafka import Consumer, KafkaException, KafkaError
from datetime import datetime, timezone, timedelta
import os

# PostgreSQL Configuration
DB_HOST = "postgres"
DB_PORT = 5432
DB_NAME = "weather_db"
DB_USER ="postgres"
DB_PASSWORD = "password"


logging.basicConfig(
    level=logging.INFO,  # Set the minimum logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format with timestamps
    handlers=[
        logging.StreamHandler(),  # Output logs to the console
        logging.FileHandler("database.log")  # Optional: Save logs to a file named 'producer.log'
    ]
)
# Kafka Consumer Configuration
'''
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'weatherdata_group',        # Consumer group ID
    'auto.offset.reset': 'earliest'         # Start from the beginning if no committed offset
}'
''
'''

consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': f'weatherdata_group_{os.getpid()}',  # Unique group per run
    'auto.offset.reset': 'earliest',  # Start from beginning
    'enable.auto.commit': False  # Disable auto-commit if needed
}

consumer = Consumer(consumer_conf)


# Database Setup Functions
def create_database_if_not_exists():
    """Creates the PostgreSQL database if it doesn't exist."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname="postgres"
        )
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
            if not cursor.fetchone():
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
                logging.info(f"Database {DB_NAME} created.")
            else:
                logging.info(f"Database {DB_NAME} already exists.")
        conn.close()
    except Exception as e:
        logging.error(f"Error while checking/creating database: {e}")
        raise

def create_table_if_not_exists(conn):
    """Creates the weather_data table if it doesn't exist."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city_name VARCHAR(50),
                date DATE,
                time TIME,
                temperature REAL,
                humidity INT,
                weather_description VARCHAR(100),
                latitude REAL,
                longitude REAL,
                UNIQUE (city_name, date, time)
            )
            """)
            conn.commit()
            logging.info("Table 'weather_data' exists or created successfully.")
    except Exception as e:
        logging.error(f"Error while checking/creating table: {e}")
        conn.rollback()

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
def insert_weather_data(conn, city, date, time, temp, humidity, description, latitude, longitude):
    """Inserts weather data including coordinates into the PostgreSQL database."""
    try:
        with conn.cursor() as cursor:
            query = """
            INSERT INTO weather_data (city_name, date, time, temperature, humidity, weather_description, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_name, date, time) DO NOTHING
            """
            cursor.execute(query, (city, date, time, temp, humidity, description, latitude, longitude))
            conn.commit()
            logging.info(f"Weather data for {city} inserted into database.")
    except Exception as e:
        logging.error(f"Error inserting data into database: {e}")
        conn.rollback()
'''
def insert_weather_data(conn, city, date, time, temp, humidity, description, latitude, longitude):
    """Inserts weather data including coordinates into the PostgreSQL database."""
    try:
        with conn.cursor() as cursor:
            query = """
            INSERT INTO weather_data (city_name, date, time, temperature, humidity, weather_description, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (city, date, time, temp, humidity, description, latitude, longitude))
            conn.commit()
            logging.info(f"Weather data for {city} inserted into database.")
    except Exception as e:
        logging.error(f"Error inserting data into database: {e}")
        conn.rollback()

# Kafka Consumer Logic
def consume_weather_data(topicname):
    """Consumes weather data from Kafka, processes it, and writes it to PostgreSQL."""
    # Ensure the database and table exist
    create_database_if_not_exists()
    conn = connect_db()
    create_table_if_not_exists(conn)

    try:
        # Subscribe to Kafka topic
        consumer.subscribe([topicname])
        logging.info("Subscribed to topic: weatherdata_topic")

        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                logging.info("No messages received.")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f"Reached end of partition: {msg.topic()}[{msg.partition()}]")
                elif msg.error():
                    logging.error(f"Kafka error: {msg.error()}")
                    raise KafkaException(msg.error())
                continue

            # Process the Kafka message
            try:
                weather_data = json.loads(msg.value().decode('utf-8'))
                logging.info(f"Received message: {weather_data}")

                # Extract required fields
                city = weather_data.get('name', 'Unknown')
                temp = weather_data['main'].get('temp')
                humidity = weather_data['main'].get('humidity')
                description = weather_data['weather'][0].get('description', 'N/A')

                # Extract coordinates
                latitude = weather_data['coord'].get('lat')
                longitude = weather_data['coord'].get('lon')
               # Format date and time
                timestamp = datetime.fromtimestamp(weather_data['dt'], tz=timezone.utc)  # Use timezone-aware UTC

                ist_offset = timedelta(hours=5, minutes=30)  # IST is UTC+5:30
                timestamp_ist = timestamp + ist_offset  # Convert to IST
            
                
                date = timestamp_ist.strftime('%Y-%m-%d')  # Standard date format
                time = timestamp_ist.strftime('%H:%M:%S')  # Standard time format

                # Insert into the database
                insert_weather_data(conn, city, date, time, temp, humidity, description, latitude, longitude)
            
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON message: {e}")
            except KeyError as e:
                logging.error(f"Missing key in weather data: {e}")

    except KeyboardInterrupt:
        logging.info("Consumer shutdown requested.")
    finally:
        consumer.close()
        conn.close()
        logging.info("Consumer and database connection closed.")


