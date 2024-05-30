import os

from pyspark.sql.types import *

SPARK_CONFIG = {
    "spark.sql.streaming.schemaInference": "true",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}

S3_CONFIG = {
    "fs.s3a.access.key": os.getenv("S3_ACCESS_KEY", "admin"),
    "fs.s3a.secret.key": os.getenv("S3_SECRET_KEY", "admin123"),
    "fs.s3a.endpoint": os.getenv("S3_ENDPOINT", "10.211.56.3:30090"),
}

KAFKA_CONFIG = {
    "bootstrap_servers": "10.211.56.2:31723,10.211.56.2:32120,10.211.56.2:32745"
}
SCHEMA_CONFIG = {
    "yellow_tripdata": StructType([
        StructField('VendorID', LongType(), True),
        StructField('tpep_pickup_datetime', StringType(), True),
        StructField('tpep_dropoff_datetime', StringType(), True),
        StructField('passenger_count', StringType(), True),
        StructField('trip_distance', DoubleType(), True),
        StructField('RatecodeID', StringType(), True),
        StructField('store_and_fwd_flag', StringType(), True),
        StructField('PULocationID', LongType(), True),
        StructField('DOLocationID', LongType(), True),
        StructField('payment_type', LongType(), True),
        StructField('fare_amount', DoubleType(), True),
        StructField('extra', DoubleType(), True),
        StructField('mta_tax', DoubleType(), True),
        StructField('tip_amount', DoubleType(), True),
        StructField('tolls_amount', DoubleType(), True),
        StructField('improvement_surcharge', DoubleType(), True),
        StructField('total_amount', DoubleType(), True),
        StructField('congestion_surcharge', DoubleType(), True),
        StructField('airport_fee', DoubleType(), True)
    ]),
    "fhvhv_tripdata": StructType([
        StructField("hvfhs_license_num", StringType(), True),
        StructField("dispatching_base_num", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("originating_base_num", StringType(), True),
        StructField("request_datetime", StringType(), True),
        StructField("on_scene_datetime", StringType(), True),
        StructField("trip_miles", DoubleType(), True),
        StructField("trip_time", DoubleType(), True),
        StructField("base_passenger_fare", DoubleType(), True),
        StructField("tolls", DoubleType(), True),
        StructField("bcf", DoubleType(), True),
        StructField("sales_tax", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("tips", DoubleType(), True),
        StructField("driver_pay", DoubleType(), True),
        StructField("shared_request_flag", StringType(), True),
        StructField("shared_match_flag", StringType(), True),
        StructField("access_a_ride_flag", StringType(), True),
        StructField("wav_request_flag", StringType(), True),
        StructField("wav_match_flag", StringType(), True),
    ]),
}
