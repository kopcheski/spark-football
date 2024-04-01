import logging

from pyspark.sql import SparkSession, Window
import requests
from datetime import datetime, timedelta
import json
import os

from pyspark.sql.functions import col, explode, row_number

from data_schema import data_schema

CACHED_DATA_JSON_FILE_NAME = "cached_data.json"
RECENT_FORM_IN_DAYS = 5
DATA_AGE_IN_DAYS = 365
DATA_RETENTION_IN_DAYS = 7

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Eredivisie data playground") \
    .getOrCreate()


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


# Convert data to Spark DataFrame (if applicable)
if cached_data:
    last_12_month_matches_df = spark.read.json(CACHED_DATA_JSON_FILE_NAME, schema=data_schema())

    # Explode the matches array to flatten it
    matches_df = last_12_month_matches_df.select(explode("matches").alias("match"))

    window_spec = Window.partitionBy(matches_df["match.homeTeam.name"]).orderBy(matches_df["match.utcDate"].desc())

    partitioned_by_home_team_df = matches_df.withColumn("row_number", row_number().over(window_spec))

    recent_home_matches_df = partitioned_by_home_team_df.filter(col("row_number") <= RECENT_FORM_IN_DAYS)

    recent_home_matches_df.createOrReplaceTempView("recent_matches")

    recent_home_form_df = spark.sql(""" 
        select
            home_team,
            sum(case 
                when all.winner = 'HOME_TEAM' then 1 
                else 0 
            end) as wins,
            sum(case 
                when all.winner = 'AWAY_TEAM' then 1 
                else 0 
            end) as losses,
            sum(case 
                when all.winner = 'DRAW' then 1 
                else 0 
            end) as draws,
            sum(all.home_scored) as goals_scored,
            sum(all.away_scored) as goals_conceded,
            avg(all.home_scored) as avg_scored,    
            avg(all.away_scored) as avg_conceded      
        from
            (select
                match.homeTeam.name as home_team,
                match.awayTeam.name as away_team,
                match.score.winner as winner,
                match.score.fullTime.home as home_scored,
                match.score.fullTime.away as away_scored                                           
            from
                recent_matches                                           
            order by
                home_team,
                match.utcDate desc) as all                      
        group by
            all.home_team                        
        order by
            all.home_team  
                            """)

    recent_home_form_df.show(n=100, truncate=False)

# Stop Spark session
spark.stop()
