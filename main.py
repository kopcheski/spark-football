from pyspark.sql import SparkSession, Window

from pyspark.sql.functions import explode, row_number, col

from data_schema import data_schema

CACHED_DATA_JSON_FILE_NAME = "cached_data.json"
RECENT_FORM_IN_DAYS = 5
DATA_RETENTION_IN_DAYS = 7
DATA_AGE_IN_DAYS = 365

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Eredivisie data playground") \
    .getOrCreate()

# Convert data to Spark DataFrame (if applicable)
last_12_month_matches_df = spark.read.json(CACHED_DATA_JSON_FILE_NAME, schema=data_schema())

# Explode the matches array to flatten it
matches_df = last_12_month_matches_df.select(explode("matches").alias("match"))


class Group:
    def __init__(self, winner):
        self.partition_by_column = "match.homeTeam.name" if winner == 'home' else "match.awayTeam.name"
        self.victory_column = 'HOME_TEAM' if winner == 'home' else "AWAY_TEAM"
        self.loss_column = 'AWAY_TEAM' if winner == 'home' else "HOME_TEAM"


groups = [Group('home'), Group('away')]
for group in groups:
    window_spec = Window.partitionBy(col(group.partition_by_column)).orderBy(col("match.utcDate").desc())

    partitioned_by_home_team_df = matches_df.withColumn("row_number", row_number().over(window_spec))

    recent_home_matches_df = partitioned_by_home_team_df.filter(
        partitioned_by_home_team_df.row_number <= RECENT_FORM_IN_DAYS)

    recent_home_matches_df.createOrReplaceTempView("recent_matches")

    recent_home_form_df = spark.sql(""" 
        select
            home_team,
            sum(case 
                when all.winner = '{fvictory_column}' then 1 
                else 0 
            end) as wins,
            sum(case 
                when all.winner = '{floss_column}' then 1 
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
                            """.format(fvictory_column=group.victory_column, floss_column=group.loss_column))

    recent_home_form_df.show(n=100, truncate=False)

# Stop Spark session
spark.stop()
