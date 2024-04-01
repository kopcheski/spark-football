from pyspark.sql.types import StructField, StructType, StringType, IntegerType, ArrayType, MapType


def data_schema():
    return StructType([
        StructField("filters", MapType(StringType(), StringType()), nullable=True),
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
