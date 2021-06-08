from pyparsing import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import json
from datetime import datetime

from config import HOSTS

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell"

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", HOSTS) \
    .option("subscribe", "all_events") \
    .load() \
    .selectExpr("CAST(value as STRING)")

data_schema = StructType([
    StructField("venue", StructType([
        StructField("venue_name", StringType(), True),
        StructField("lon", FloatType(), True),
        StructField("lat", FloatType(), True),
        StructField("venue_id", IntegerType(), True)
    ])),
    StructField("visibility", StringType(), True),
    StructField("response", StringType(), True),
    StructField("guests", IntegerType(), True),
    StructField("member", StructType([
        StructField("member_id", IntegerType(), True),
        StructField("photo", StringType(), True),
        StructField("member_name", StringType(), True)
    ])),
    StructField("rsvp_id", IntegerType(), True),
    StructField("mtime", IntegerType(), True),
    StructField("event", StructType([
        StructField("event_name", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("time", TimestampType(), True),
        StructField("event_url", StringType(), True),
    ])),
    StructField("group", StructType([
        StructField("group_topics", ArrayType(
            StructType([
                StructField("urlkey", StringType(), False),
                StructField("topic_name", StringType(), False)
            ]), True
        ), True),
        StructField("group_city", StringType(), True),
        StructField("group_country", StringType(), True),
        StructField("group_id", IntegerType(), False),
        StructField("group_name", StringType(), True),
        StructField("group_lon", FloatType(), True),
        StructField("group_urlname", StringType(), True),
        StructField("group_state", StringType(), True),
        StructField("group_lat", FloatType(), True)
    ]))
])

df_schema = df.select(from_json("value", data_schema).alias("data"))
all_data = df_schema.select("data.*")
only_country_data = df_schema.select("data.group.group_country")

# You should filter the records in order to take only the meetups from the US ("group_country":"us").
Q1 = df_schema.select("data.*").where("group.group_country = \"us\"")

# Send all the events which are happening in the US to the topic US-meetups.
q2_data = Q1.select(
    "group.group_city",
    "group.group_country",
    "group.group_id",
    "group.group_name",
    "group.group_state",
    "event.event_name",
    "event.event_id",
    "event.time"
)

states = [
    ("ALABAMA", "AL"),
    ("ALASKA", "AK"),
    ("AMERICAN SAMOA", "AS"),
    ("ARIZONA", "AZ"),
    ("ARKANSAS", "AR"),
    ("CALIFORNIA", "CA"),
    ("COLORADO", "CO"),
    ("CONNECTICUT", "CT"),
    ("DELAWARE", "DE"),
    ("DISTRICT OF COLUMBIA", "DC"),
    ("FLORIDA", "FL"),
    ("GEORGIA", "GA"),
    ("GUAM", "GU"),
    ("HAWAII", "HI"),
    ("IDAHO", "ID"),
    ("ILLINOIS", "IL"),
    ("INDIANA", "IN"),
    ("IOWA", "IA"),
    ("KANSAS", "KS"),
    ("KENTUCKY", "KY"),
    ("LOUISIANA", "LA"),
    ("MAINE", "ME"),
    ("MARYLAND", "MD"),
    ("MASSACHUSETTS", "MA"),
    ("MICHIGAN", "MI"),
    ("MINNESOTA", "MN"),
    ("MISSISSIPPI", "MS"),
    ("MISSOURI", "MO"),
    ("MONTANA", "MT"),
    ("NEBRASKA", "NE"),
    ("NEVADA", "NV"),
    ("NEW HAMPSHIRE", "NH"),
    ("NEW JERSEY", "NJ"),
    ("NEW MEXICO", "NM"),
    ("NEW YORK", "NY"),
    ("NORTH CAROLINA", "NC"),
    ("NORTH DAKOTA", "ND"),
    ("NORTHERN MARIANA IS", "MP"),
    ("OHIO", "OH"),
    ("OKLAHOMA", "OK"),
    ("OREGON", "OR"),
    ("PENNSYLVANIA", "PA"),
    ("PUERTO RICO", "PR"),
    ("RHODE ISLAND", "RI"),
    ("SOUTH CAROLINA", "SC"),
    ("SOUTH DAKOTA", "SD"),
    ("TENNESSEE", "TN"),
    ("TEXAS", "TX"),
    ("UTAH", "UT"),
    ("VERMONT", "VT"),
    ("VIRGINIA", "VA"),
    ("VIRGIN ISLANDS", "VI"),
    ("WASHINGTON", "WA"),
    ("WEST VIRGINIA", "WV"),
    ("WISCONSIN", "WI"),
    ("WYOMING", "WY")
]
states_columns = ["group_state", "state_short"]
states_df = spark.createDataFrame(states, states_columns)

Q2 = q2_data.join(states_df, q2_data.group_state == states_df.state_short, 'inner').select(
    to_json(struct("event_id", "event_name", "time")).alias("event"),
    q2_data.group_city, q2_data.group_country, q2_data.group_id, q2_data.group_name, states_df.group_state)

Q2 = Q2.select(to_json(struct("event", "group_city", "group_country", "group_id", "group_state")).alias("value"))

Q2.selectExpr("CAST(value AS STRING)")\
    .writeStream \
    .format("kafka")\
    .option("kafka.bootstrap.servers", HOSTS)\
    .option("checkpointLocation", "checkpoint")\
    .option("topic", "us_meetups") \
    .start() \
    .awaitTermination()

# Q2.writeStream \
#     .format("console")\
#     .start() \
#     .awaitTermination()
