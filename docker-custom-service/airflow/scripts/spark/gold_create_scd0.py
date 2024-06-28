from core_dwh import *
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import logging
from spark_session_init import create_spark_session
import argparse
import os

S3_ENDPOINT = os.environ.get('S3_ENDPOINT', "http://minio.minio.svc.cluster.local:9000")
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY', "admin")
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY', "admin123")
KAFKA_CONSUMER_SERVER = os.environ.get('KAFKA_CONSUMER_SERVER', "kafka.kafka.svc.cluster.local:9092")
BUCKET_NAME = os.environ.get('BUCKET_NAME', "nyc-trip-bucket")

if __name__ == "__main__":
    spark = create_spark_session(app_name="Create SCD0", s3_endpoint=S3_ENDPOINT,
                                 s3_access_key=S3_ACCESS_KEY, s3_secret_key=S3_SECRET_KEY)
    silver_location = f"s3a://{BUCKET_NAME}/silver"
    gold_location = f"s3a://{BUCKET_NAME}/gold"
    builder = DimensionTableBuilder(silver_location=silver_location, dwh_location=gold_location, spark=spark)
    builder.run_dim_builder()
