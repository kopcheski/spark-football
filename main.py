from pyspark.sql import SparkSession
import requests
from datetime import datetime, timedelta
import json
import os

from data import data_schema

CACHED_DATA_JSON_FILE_NAME = "cached_data.json"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Fetch Data from Endpoint with Caching") \
    .getOrCreate()


def fetch_data():
    request_url = "http://api.football-data.org/v4/competitions/DED/matches"
    headers = {'X-Auth-Token': os.environ.get("FOOTBALL_DATA_ORG_TOKEN")}
    query_params = {'dateFrom': (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
                    'dateTo': datetime.now().strftime('%Y-%m-%d')}

    # Construct the request URL with query parameters if provided
    if query_params:
        request_url += "?" + "&".join([f"{k}={v}" for k, v in query_params.items()])

    # Make the request with headers if provided
    response = requests.get(request_url, headers=headers)

    # Check if request was successful
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None


# Function to check if data needs to be refreshed
def needs_refresh(previous_refresh_time):
    # Check if previous refresh time is at least a week old
    return datetime.now() - previous_refresh_time >= timedelta(days=7)


# Function to fetch data and cache it if necessary
def fetch_and_cache_data():
    # Check if data needs to be refreshed
    if not needs_refresh(fetch_and_cache_data.previous_refresh_time):
        return fetch_and_cache_data.cached_data

    # Fetch new data from the endpoint
    new_data = fetch_data()

    # Cache the new data
    fetch_and_cache_data.cached_data = new_data
    fetch_and_cache_data.previous_refresh_time = datetime.now()

    return new_data


# Initialize cached data and previous refresh time
fetch_and_cache_data.cached_data = None
fetch_and_cache_data.previous_refresh_time = datetime.min

# Check if cached data exists and is not empty
if os.path.exists(CACHED_DATA_JSON_FILE_NAME) and os.stat(CACHED_DATA_JSON_FILE_NAME).st_size > 0:
    with open(CACHED_DATA_JSON_FILE_NAME, "r") as file:
        fetch_and_cache_data.cached_data = json.load(file)
        fetch_and_cache_data.previous_refresh_time = datetime.fromtimestamp(os.path.getmtime(CACHED_DATA_JSON_FILE_NAME))

# Fetch and cache data
cached_data = fetch_and_cache_data()

# Save cached data to file
if cached_data:
    with open(CACHED_DATA_JSON_FILE_NAME, "w") as file:
        json.dump(cached_data, file)


# Convert data to Spark DataFrame (if applicable)
if cached_data:
    schema = data_schema()
    cached_data_as_str = json.dumps(cached_data)
    df = spark.read.schema(schema).json(cached_data_as_str)
    df.show()

# Stop Spark session
spark.stop()
