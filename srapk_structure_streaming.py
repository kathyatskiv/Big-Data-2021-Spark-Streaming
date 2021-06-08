from pyparsing import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, concat, col, lit
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
        StructField("group_lat", FloatType(), True)
    ]))
])

df_schema = df.select(from_json("value", data_schema).alias("data"))
all_data = df_schema.select("data.*")
only_country_data = df_schema.select("data.group.group_country")

# You should filter the records in order to take only the meetups from the US ("group_country":"us").
Q1 = df_schema.select("data.*").where("group.group_country = \"us\"")

# Send all the events which are happening in the US to the topic US-meetups.
q2_schema = StructType([
    StructField("event", StructType([
        StructField("event_name", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("time", StringType(), False)
    ]), False),
    StructField("group_city", StringType(), False),
    StructField("group_country", StringType(), False),
    StructField("group_id", IntegerType(), False),
    StructField("group_name", StringType(), False),
    StructField("group_state", StringType(), True)
])

events = Q1.select("event.event_name", "event.event_id", "event.time")

q2_data = Q1.select(
    "group.group_city",
    "group.group_country",
    "group.group_id",
    "group.group_name",
    "event.event_name",
    "event.event_id",
    "event.time"
)

Q2 = q2_data.withColumn("event", concat(
        lit("{ event_name: "),
        col("event_name"),
        lit(", event_id: "),
        col("event_id"),
        lit(", time: "),
        col("time")))\
    .withColumn("group_city", col("group_city"))\
    .withColumn("group_country", col("group_country"))\
    .withColumn("group_id", col("group_id"))\
    .withColumn("group_name", col("group_name"))
df.show()
df.printSchema()

Q2.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()
