import json
from confluent_kafka import Producer
import requests
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the minimum logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format with timestamps
    handlers=[
        logging.StreamHandler(),  # Output logs to the console
        logging.FileHandler("producer.log")  # Optional: Save logs to a file named 'producer.log'
    ]
)

'''
# Kafka Producer Configuration
producer_conf = {
    'bootstrap.servers': 'kafkaweatherexp1:9092',
    'linger.ms': 100,  # Delay up to 100ms to batch messages
    'batch.size': 16384  # Batch up to 16KB before sending
}
'''
producer_conf = {
    'bootstrap.servers': 'kafka:9092',
    }
producer = Producer(producer_conf)

def fetch_weather_data(city_name, API_key):
    """Fetches real-time weather data from OpenWeather API."""
    base_url = f"http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city_name,
        "appid": API_key,
        "units": "metric"  # Get temperature in Celsius
    }

    try:
        # Perform the API request
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)
        logging.info(f"Fetched weather data for {city_name}")
        return response.json()  # Return the parsed JSON data
    except requests.exceptions.RequestException as e:
        # Catch any request-related error and log it
        logging.error(f"Error occurred while fetching weather data for {city_name}: {e}")
        return None


def delivery_report(err, msg):
    """Callback for delivery reports from Kafka producer."""
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_weather_data(city_name, API_KEY, timeinterval, topicname):
    """Fetches weather data and sends it to Kafka."""
    try:
        while True:
            weather_data = fetch_weather_data(city_name, API_KEY)
            if weather_data:
                # Send data to Kafka topic
                producer.produce(topicname, value=json.dumps(weather_data), callback=delivery_report)
                logging.info(f"Sent weather data for {city_name}")
            else:
                logging.warning(f"No data fetched for {city_name}")
            
            time.sleep(timeinterval)  # Fetch data every "timeinterval" seconds
    except Exception as e:
        logging.error(f"Error in producing weather data: {e}")
    finally:
        producer.flush()
        logging.info("All messages flushed to Kafka")

