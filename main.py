from pyspark.sql import SparkSession
import requests
from datetime import datetime, timedelta
import json
import os

from pyspark.sql.types import StructField, IntegerType, StringType, StructType, ArrayType

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
if os.path.exists("cached_data.json") and os.stat("cached_data.json").st_size > 0:
    with open("cached_data.json", "r") as file:
        fetch_and_cache_data.cached_data = json.load(file)
        fetch_and_cache_data.previous_refresh_time = datetime.fromtimestamp(os.path.getmtime("cached_data.json"))

# Fetch and cache data
cached_data = fetch_and_cache_data()

# Save cached data to file
if cached_data:
    with open("cached_data.json", "w") as file:
        json.dump(cached_data, file)

# Convert data to Spark DataFrame (if applicable)
if cached_data:
    schema = StructType([
        StructField("filters", StructType([
            StructField("season", StringType(), nullable=True)
        ]), nullable=True),
        StructField("resultSet", StructType([
            StructField("count", IntegerType(), nullable=True),
            StructField("first", StringType(), nullable=True),
            StructField("last", StringType(), nullable=True),
            StructField("played", IntegerType(), nullable=True)
        ]), nullable=True),
        StructField("competition", StructType([
            StructField("id", IntegerType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("code", StringType(), nullable=True),
            StructField("type", StringType(), nullable=True),
            StructField("emblem", StringType(), nullable=True)
        ]), nullable=True),
        StructField("matches", ArrayType(StructType([
            StructField("area", StructType([
                StructField("id", IntegerType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                StructField("code", StringType(), nullable=True),
                StructField("flag", StringType(), nullable=True)
            ]), nullable=True),
            StructField("competition", StructType([
                StructField("id", IntegerType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                StructField("code", StringType(), nullable=True),
                StructField("type", StringType(), nullable=True),
                StructField("emblem", StringType(), nullable=True)
            ]), nullable=True),
            StructField("season", StructType([
                StructField("id", IntegerType(), nullable=True),
                StructField("startDate", StringType(), nullable=True),
                StructField("endDate", StringType(), nullable=True),
                StructField("currentMatchday", IntegerType(), nullable=True),
                StructField("winner", StringType(), nullable=True)
            ]), nullable=True),
            StructField("id", IntegerType(), nullable=True),
            StructField("utcDate", StringType(), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("matchday", IntegerType(), nullable=True),
            StructField("stage", StringType(), nullable=True),
            StructField("group", StringType(), nullable=True),
            StructField("lastUpdated", StringType(), nullable=True),
            StructField("homeTeam", StructType([
                StructField("id", IntegerType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                StructField("shortName", StringType(), nullable=True),
                StructField("tla", StringType(), nullable=True),
                StructField("crest", StringType(), nullable=True)
            ]), nullable=True),
            StructField("awayTeam", StructType([
                StructField("id", IntegerType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                StructField("shortName", StringType(), nullable=True),
                StructField("tla", StringType(), nullable=True),
                StructField("crest", StringType(), nullable=True)
            ]), nullable=True),
            StructField("score", StructType([
                StructField("winner", StringType(), nullable=True),
                StructField("duration", StringType(), nullable=True),
                StructField("fullTime", StructType([
                    StructField("home", IntegerType(), nullable=True),
                    StructField("away", IntegerType(), nullable=True)
                ]), nullable=True),
                StructField("halfTime", StructType([
                    StructField("home", IntegerType(), nullable=True),
                    StructField("away", IntegerType(), nullable=True)
                ]), nullable=True)
            ]), nullable=True),
            StructField("odds", StructType([
                StructField("msg", StringType(), nullable=True)
            ]), nullable=True),
            StructField("referees", ArrayType(StructType([
                StructField("id", IntegerType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                StructField("type", StringType(), nullable=True),
                StructField("nationality", StringType(), nullable=True)
            ])), nullable=True)
        ])), nullable=True)
    ])
    cached_data_as_str = json.dumps(cached_data)
    df = spark.read.schema(schema).json(cached_data_as_str)
    df.show()

# Stop Spark session
spark.stop()