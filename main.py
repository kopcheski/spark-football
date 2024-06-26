from pyspark.sql import SparkSession, Window

from pyspark.sql.functions import explode, row_number, col, count

from data_schema import data_schema

CACHED_DATA_JSON_FILE_NAME = "cached_data.json"
RECENT_FORM_IN_X_MATCHES = 5
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
    def __init__(self, playing_as):
        self.partition_by_column = "match.homeTeam.name" if playing_as == 'home' else "match.awayTeam.name"
        self.victory_column = 'HOME_TEAM' if playing_as == 'home' else "AWAY_TEAM"
        self.loss_column = 'AWAY_TEAM' if playing_as == 'home' else "HOME_TEAM"
        self.label = 'home' if playing_as == 'home' else "away"


groups = [Group('home'), Group('away')]
for group in groups:
    partition_column = col(group.partition_by_column)
    utc_date_desc = col("match.utcDate").desc()
    window_spec = Window.partitionBy(partition_column).orderBy(utc_date_desc)

    partitioned_by_team_df = matches_df.withColumn("row_number", row_number().over(window_spec))

    recent_matches_df = partitioned_by_team_df.filter(
        col("row_number") <= RECENT_FORM_IN_X_MATCHES)

    recent_matches_df.createOrReplaceTempView("recent_matches")

    recent_form_df = spark.sql(""" 
        select
            {flabel_column}_team as team,
            sum(case 
                when all.winner = '{fvictory_column}' then 1 
                else 0 
            end) as wins,
            sum(case 
                when all.winner = 'DRAW' then 1 
                else 0 
            end) as draws,
            sum(case 
                when all.winner = '{floss_column}' then 1 
                else 0 
            end) as losses,
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
            all.{flabel_column}_team                        
        order by
            all.{flabel_column}_team  
                            """.format(fvictory_column=group.victory_column,
                                       floss_column=group.loss_column,
                                       flabel_column=group.label))

    recent_form_df.createOrReplaceTempView("recent_form_{fgroup}".format(fgroup=group.label))

    recent_form_df.show(n=100, truncate=False)

recent_overall_form_df = spark.sql("""
    select
        team,
        sum(wins) as wins,
        sum(draws) as draws,
        sum(losses) as losses,
        sum(goals_scored) as scored,
        sum(goals_conceded) as conceded,
        sum(avg_scored) as avg_scored,
        sum(avg_conceded) as avg_conceded,
        ((100 * wins * 3 + draws)/({matches} * 3)) as pctg
    from
        (
                select
                    team,
                    wins,
                    draws,
                    losses,
                    goals_scored,
                    goals_conceded,
                    avg_scored,
                    avg_conceded
                from
                    recent_form_home
                
                UNION ALL
                
                select
                    team,
                    wins,
                    draws,
                    losses,
                    goals_scored,
                    goals_conceded,
                    avg_scored,
                    avg_conceded
                from
                    recent_form_away ) as recent_form
    group by
        team, pctg
    order by pctg DESC
    """.format(matches=RECENT_FORM_IN_X_MATCHES))

recent_overall_form_df.show(n=100, truncate=False)

# Stop Spark session
spark.stop()
