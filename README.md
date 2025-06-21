## 🌦️A Simple Weather Data Pipeline | My Data Engineering Learning Project
A beginner-friendly Python Script to practice data pipelines. Fetches live weather data from OpenWeatherMap, processes it, and stores it in SQLite.

## How It Works
1. Fetches the weather for some selected cities in the world via API
2. Converts and cleans the data
3. Saves everything to a local database

## Setup
1. Get a free API key from [OpenWeatherMap](https://openweathermap.org/)
2. Set it as "OWM_API_KEY" in your  environment variables
3. Run `python weather_etl.py`
