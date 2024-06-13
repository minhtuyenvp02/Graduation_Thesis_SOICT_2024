import logging
from pyspark.sql import SparkSession
from silver_medallion import Silver
from config import *
from spark_executor import create_spark_session
import argparse

if __name__ == "__main__":
    spark = create_spark_session(app_name="Silver FHVHV Transform",
                                 s3_endpoint="http://minio.minio.svc.cluster.local:9000", s3_access_key='admin',
                                 s3_secret_key='admin123')
    silver = Silver(bucket_name='nyc-trip-bucket', spark=spark)
    silver.fhvhv_transform()
