import os

from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

SPARK_CONFIG = {
    "spark.sql.streaming.schemaInference": "true",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}

S3_CONFIG = {
    "fs.s3a.access.key": os.getenv("S3_ACCESS_KEY", "admin"),
    "fs.s3a.secret.key": os.getenv("S3_SECRET_KEY", "admin123"),
    "fs.s3a.endpoint": os.getenv("S3_ENDPOINT", "10.211.56.7:30090"),
}

EXTRA_JAR_PACKAGE = [
    'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1']

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "10.211.56.7:31463")
}
SCHEMA_CONFIG = {
    "yellow_tripdata": StructType([
        StructField('VendorID', LongType(), True),
        StructField('tpep_pickup_datetime', StringType(), True),
        StructField('tpep_dropoff_datetime', StringType(), True),
        StructField('passenger_count', DoubleType(), True),
        StructField('trip_distance', DoubleType(), True),
        StructField('RatecodeID', DoubleType(), True),
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
        StructField('congestion_surcharge', DoubleType(), True)
    ]),
    "green_tripdata": StructType([
        StructField('VendorID', LongType(), True),
        StructField('Ipep_pickup_datetime', StringType(), True),
        StructField('Ipep_dropoff_datetime', StringType(), True),
        StructField('Passenger_count', DoubleType(), True),
        StructField('Trip_distance', DoubleType(), True),
        StructField('PULocationID', LongType(), True),
        StructField('DOLocationID', LongType(), True),
        StructField('RatecodeID', DoubleType(), True),
        StructField('store_and_fwd_flag', StringType(), True),
        StructField('Payment_type', LongType(), True),
        StructField('Fare_amount', DoubleType(), True),
        StructField('Extra', DoubleType(), True),
        StructField('MTA_tax', DoubleType(), True),
        StructField('Improvement_surcharge', DoubleType(), True),
        StructField('Tip_amount', DoubleType(), True),
        StructField('Tolls_amount', DoubleType(), True),
        StructField('Total_amount', DoubleType(), True),
        StructField('Trip_type', StringType(), True)
    ]),
    "fhv_tripdata":
        StructType([
            StructField("Dispatching_base_num", LongType(), True),
            StructField("Pickup_datetime", StringType(), True),
            StructField("DropOff_datetime", StringType(), True),
            StructField("PULocationID", LongType(), True),
            StructField("DOLocationID", LongType(), True),
            StructField("SR_Flag", StringType(), True)]),
    "fhvhv_tripdata": StructType([
        StructField("Hvfhs_license_num ", StringType(), True),
        StructField("Dispatching_base_num ", LongType(), True),
        StructField("Pickup_datetime", StringType(), True),
        StructField("DropOff_datetime", StringType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("originating_base_num", LongType(), True),
        StructField("request_datetime", StringType(), True),
        StructField("on_scene_datetime", StringType(), True),
        StructField("trip_miles", DoubleType(), True),
        StructField("trip_time", StringType(), True),
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
