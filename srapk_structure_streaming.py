from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
import os

from config import HOSTS

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell"

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", HOSTS)\
    .option("subscribe", "all_events")\
    .load()\
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
Q1 = df_schema.select("data.*").where("group.group_country = \"us\"")

query = Q1.writeStream \
        .format("console") \
        .start() \
        .awaitTermination()
