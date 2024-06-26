from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from config import *
import logging
from spark_session_init import create_spark_session
from gold_medallion import GoldDataProessing
import argparse
import os
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', "http://minio.minio.svc.cluster.local:9000")
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY', "admin")
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY', "admin123")
KAFKA_CONSUMER_SERVER = os.environ.get('KAFKA_CONSUMER_SERVER', "kafka.kafka.svc.cluster.local:9092")
BUCKET_NAME = os.environ.get('BUCKET_NAME', "nyc-trip-bucket")

if __name__ == "__main__":

    spark = create_spark_session(app_name="update-fact-yellow-tracking",
                                 s3_endpoint=S3_ENDPOINT, s3_access_key=S3_ACCESS_KEY,
                                 s3_secret_key=S3_SECRET_KEY)
    gold_processing = GoldDataProessing(bucket_name=BUCKET_NAME, spark=spark)
    try:
        gold_procesing.update_fact_yellow_tracking_location_daily()
    except Exception as E:
        logging.info("Failed to load data to gold")
        logging.info(E)
