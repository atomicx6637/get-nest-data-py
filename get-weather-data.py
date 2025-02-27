import os
import json
import logging
import requests
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta

# Load configuration from external JSON file
CONFIG_PATH = '/home/trichard/projects/get-nest-data/weather_config.json'
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"Configuration file not found: {CONFIG_PATH}")

with open(CONFIG_PATH, 'r') as config_file:
    config = json.load(config_file)

DB_CONFIG = config.get('db')
API_KEY = config.get('weather_api', {}).get('api_key')
STATION_ID = config.get('weather_api', {}).get('station_id')
DAYS_BACK = config.get('days_back', 7)
LOGGING_FILE_PATH = config.get('logging_file_path')

# Setup logging to log to both console and an external file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGGING_FILE_PATH + 'weather_data.log')
    ]
)

def fetch_weather_data(date_api):
    """
    Fetch hourly weather data from Weather Underground for a given date (YYYYMMDD).
    """
    url = (f"https://api.weather.com/v2/pws/history/hourly?stationId={STATION_ID}"
           f"&format=json&units=e&date={date_api}&apiKey={API_KEY}")
    response = requests.get(url)
    if response.status_code == 200:
        logging.info(f"Successfully fetched data for {date_api}")
        return response.json()
    else:
        logging.error(f"Error fetching data for {date_api}: {response.status_code}")
        return None

def calculate_hourly_avg(data, target_date_db):
    """
    Process the API data and calculate average hourly temperature.
    
    target_date_db: string in YYYY-MM-DD format for database insertion.
    """
    if not data or 'observations' not in data:
        logging.error(f"No valid data for {target_date_db}")
        return None

    hourly_data = []
    for obs in data['observations']:
        try:
            obs_time = obs.get('obsTimeLocal')
            if not obs_time:
                continue

            # Parse the local observation time
            dt = datetime.strptime(obs_time, "%Y-%m-%d %H:%M:%S")
            hour = dt.hour

            # Extract the average temperature from the 'imperial' key
            temp = obs.get('imperial', {}).get('tempAvg')
            if temp is not None:
                hourly_data.append({'date': target_date_db, 'hour': hour, 'temperature': temp})
        except (KeyError, ValueError) as e:
            logging.error(f"Skipping entry due to error: {e}")

    if not hourly_data:
        logging.error(f"No valid temperature data for {target_date_db}")
        return None

    df = pd.DataFrame(hourly_data)
    # Group by date and hour and calculate the average temperature
    return df.groupby(['date', 'hour'])['temperature'].mean().reset_index()

def upsert_into_db(data):
    """
    Insert or update processed weather data in MySQL database using UPSERT logic.
    """
    if data is None or data.empty:
        return

    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        upsert_query = """
            INSERT INTO weather_hourly (station_id, date, hour, avg_temperature)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE avg_temperature = VALUES(avg_temperature);
        """

        for _, row in data.iterrows():
            cursor.execute(upsert_query, (STATION_ID, row['date'], row['hour'], row['temperature']))

        connection.commit()
        cursor.close()
        connection.close()
        logging.info(f"Upserted {len(data)} records for {data.iloc[0]['date']} into MySQL.")
    except mysql.connector.Error as err:
        logging.error(f"MySQL error: {err}")

# Main execution loop for X days back
for days in range(DAYS_BACK):
    # For the API, use YYYYMMDD format; for the DB, use YYYY-MM-DD format.
    target_date_api = (datetime.today() - timedelta(days=days)).strftime('%Y%m%d')
    target_date_db  = (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    logging.info(f"Processing data for {target_date_db} (API date: {target_date_api})...")
    
    weather_data = fetch_weather_data(target_date_api)
    avg_hourly_temps = calculate_hourly_avg(weather_data, target_date_db)
    
    if avg_hourly_temps is not None:
        upsert_into_db(avg_hourly_temps)

logging.info("Data processing completed.")

