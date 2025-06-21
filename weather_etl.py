import os
import requests
import pandas as pd
import sqlite3
from datetime import datetime

# Function to get weather data from OpenWeatherMap

API_KEY = os.getenv('OWM_API_KEY')

if not API_KEY:
    raise ValueError("OpenWeatherMap API key not found in environment variables.")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
CITIES = ["London","Paris","Tokyo","New York","Sydney"]
DB_NAME = "weather_data.db"

#function to convert Kelvin to Celsius

def kelvin_to_celsius(kelvin):
    return kelvin - 273.15

#create a table to store the data

def create_table(cursor):
    cursor.execute("""
CREATE TABLE IF NOT EXISTS weather_data (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   city TEXT NOT NULL,
                   temperature_celsius REAL,
                   feels_like_celsius REAL,
                   temp_min_celsius REAL,
                   temp_max_celsius REAL,
                   pressure INTEGER,
                   humidity INTEGER,
                   wind_speed REAL,
                   cloudiness_percentage INTEGER,
                   visibility_meters REAL,
                   sunrise_time DATETIME,
                   sunset_time DATETIME,
                   country TEXT NOT NULL,
                   weather_desc TEXT NOT NULL,
                   record_time DATETIME DEFAULT CURRENT_TIMESTAMP)"""
)
    
#setting up the extraction of data
    
def extract_data(city, api_key, base_url):
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric",
        "lang": "en",
    }
    try:
        response = requests.get(base_url,params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as a:
        print(f"Error fetching data from {city}: {a}")
        return None

#Transforming the data

def transform_data(raw_data):
    if not raw_data:
        return None
    if 'main' not in raw_data or 'weather' not in raw_data or 'wind' not in raw_data:
        print(f" warning: missing required data in {raw_data.get('name', 'unknown city')}.Skipping transformation.")
        return None
    city = raw_data.get('name', 'Unknown city')
    country = raw_data.get('sys', {}).get('country', 'N/A')

    weather_desc = raw_data['weather'][0]["description"] if raw_data["weather"] else "N/A"
    temperature_k = raw_data['main']['temp']
    feels_like_k = raw_data['main']['feels_like']
    temp_min_k = raw_data['main']['temp_min']
    temp_max_k = raw_data['main']['temp_max']

    temperature_c = kelvin_to_celsius(temperature_k)
    feels_like_c = kelvin_to_celsius(feels_like_k)
    temp_min_c = kelvin_to_celsius(temp_min_k)
    temp_max_c = kelvin_to_celsius(temp_max_k)

    pressure = raw_data['main']['pressure']
    humidity = raw_data['main']['humidity']
    wind_speed = raw_data['wind']['speed']
    cloudiness_percentage = raw_data.get('clouds', {}).get('all', 0)
    visibility_meters = raw_data['visibility']

    sunrise_time = datetime.fromtimestamp(raw_data['sys']['sunrise']).strftime("%Y-%m-%d %H:%M:%S") if "sunrise" in raw_data['sys'] else None
    sunset_time = datetime.fromtimestamp(raw_data['sys']['sunset']).strftime("%Y-%m-%d %H:%M:%S") if "sunset" in raw_data['sys'] else None

    record_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    transform_data = {
        'city': city,
        'country': country,
        'temperature_celsius': temperature_c,
        'feels_like_celsius': feels_like_c,
        'temp_min_celsius': temp_min_c,
        'temp_max_celsius': temp_max_c,
        'pressure': pressure,
        'humidity': humidity,
        'wind_speed': wind_speed,
        'cloudiness_percentage': cloudiness_percentage,
        'visibility_meters': visibility_meters,
        'sunrise_time': sunrise_time,
        'sunset_time': sunset_time,
        'record_time': record_time,
        'weather_desc': weather_desc,
    }
    return transform_data

#loading data into the database

def load_data_into_db(data, db_name):
    if not data:
        print("No data to load...")
        return
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        cur = conn.cursor()
        create_table(cur)

        cols = ', '.join(data.keys())
        placeholders =':' + ', :'.join(data.keys())
        sql = f"INSERT INTO weather_data ({cols}) VALUES ({placeholders})"

        cur.execute(sql, data)
        conn.commit()
        print(f"sucessfully loaded the data into the database {db_name}")

    except sqlite3.Error as e:
        print(f"Error loading data into the database: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
        print("Finished processing weather data.")
    
#ochestration of the data pipeline
#what does ochestration mean as a beginner?
#ochestration is the process of coordinating multiple applications, services, or components to work together effectively.
        
def run_weather_data_pipeline():
    all_transformed_data = []
    print("startring weather data pipeline...")

    for city in CITIES:
        print(f"Processing data for: {city}")
        raw_data = extract_data(city, API_KEY, BASE_URL)
        if raw_data:
            transformed_data = transform_data(raw_data)
            if transformed_data:
                all_transformed_data.append(transformed_data)
            else:
                print(f"Transformed failed for: {city}.Skipping to the next city.")
        else:
            print(f"Extraction failed for: {city}. Skipping transformation and loading.")
    for record in all_transformed_data:
        load_data_into_db(record, DB_NAME)

    print("Finished processing weather data pipeline.")

if __name__ == "__main__":
    run_weather_data_pipeline()





    



