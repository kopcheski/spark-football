import json
import logging
import os
from datetime import datetime, timedelta

from pyspark.resource import requests

from main import CACHED_DATA_JSON_FILE_NAME, DATA_AGE_IN_DAYS, DATA_RETENTION_IN_DAYS


def fetch_data():
    logging.info("Fetching online data.")
    request_url = "http://api.football-data.org/v4/competitions/DED/matches"
    headers = {'X-Auth-Token': os.environ.get("FOOTBALL_DATA_ORG_TOKEN")}
    query_params = {'dateFrom': (datetime.now() - timedelta(days=DATA_AGE_IN_DAYS)).strftime('%Y-%m-%d'),
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
    return datetime.now() - previous_refresh_time >= timedelta(days=DATA_RETENTION_IN_DAYS)


# Function to fetch data and cache it if necessary
def fetch_and_cache_data():
    # Check if data needs to be refreshed
    if not needs_refresh(fetch_and_cache_data.previous_refresh_time):
        logging.info("Using cached data.")
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
logging.basicConfig(level=logging.INFO)

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