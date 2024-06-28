from pyspark.sql import SparkSession
from bronze_medallion import BronzeData
from config import *
from spark_session_init import create_spark_session
from schema import CustomSchema
import os
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', "http://minio.minio.svc.cluster.local:9000")
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY', "admin")
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY', "admin123")
KAFKA_CONSUMER_SERVER = os.environ.get('KAFKA_CONSUMER_SERVER', "kafka.kafka.svc.cluster.local:9092")
BUCKET_NAME = os.environ.get('BUCKET_NAME', "nyc-trip-bucket")

if __name__ == "__main__":
    schema = CustomSchema(SCHEMA_CONFIG)
    spark = create_spark_session(app_name="Yellow Trip To Bronze",
                                 s3_endpoint=S3_ENDPOINT, s3_access_key=S3_ACCESS_KEY,
                                 s3_secret_key=S3_SECRET_KEY)
    bronze_processing = BronzeDataProcessing(schema=schema, kafka_server=KAFKA_CONSUMER_SERVER, bucket_name=BUCKET_NAME, spark=spark
                        )
    bronze_processing.stream_data_to_bronze(topic='yellow_tripdata')