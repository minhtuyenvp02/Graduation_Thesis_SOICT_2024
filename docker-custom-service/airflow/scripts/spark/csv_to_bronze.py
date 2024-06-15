from pyspark.sql import SparkSession
from bronze_medallion import BronzeData
from config import *
from spark_executor import create_spark_session
from datetime import datetime
from schema import CustomSchema
import os
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', "http://minio.minio.svc.cluster.local:9000")
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY', "admin")
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY', "admin123")
KAFKA_CONSUMER_SERVER = os.environ.get('KAFKA_CONSUMER_SERVER', "kafka.kafka.svc.cluster.local:9092")
BUCKET_NAME = os.environ.get('BUCKET_NAME', "nyc-trip-bucket")
LOCATION_CSV_PATH = os.environ.get('LOCATION_CSV_PATH', "s3a://nyc-trip-bucket/nyc-data/location.csv")
DPC_BASE_NUM_CSV_PATH = os.environ.get("DPC_BASE_NUM_CSV_PATH", "s3a://nyc-trip-bucket/nyc-data/dpc_base_num.csv")
if __name__ == "__main__":
    schema = CustomSchema(SCHEMA_CONFIG)
    spark = create_spark_session(app_name="CSV To Bronze",
                                 s3_endpoint=S3_ENDPOINT, s3_access_key=S3_ACCESS_KEY,
                                 s3_secret_key=S3_SECRET_KEY)
    bronze = BronzeData(schema=schema, kafka_server=KAFKA_CONSUMER_SERVER, bucket_name=BUCKET_NAME, spark=spark
                        )
    bronze.csv_to_bronze(source_csv=LOCATION_CSV_PATH, target_table_name="location",
                         id_option=False)
    bronze.csv_to_bronze(source_csv=DPC_BASE_NUM_CSV_PATH,
                         target_table_name="dpc_base_num", id_option=True)