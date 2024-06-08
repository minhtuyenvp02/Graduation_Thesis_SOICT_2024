import logging
import os
import string
import uuid
import argparse
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.functions import *
from confluent_kafka.admin import AdminClient
from pyspark.sql import SparkSession
from delta import *
from minio import Minio
from schema import CustomSchema
from spark_executor import create_spark_session
from config import SCHEMA_CONFIG


if __name__ == "__main__":
    args = parser.parse_args()
    schema = CustomSchema(SCHEMA_CONFIG)
    spark = create_spark_session(app_name="Kafka Stream To Bronze", spark_cluster=args.spark_cluster,
                                 s3_endpoint=args.s3_endpoint, s3_access_key=args.s3_access_key,
                                 s3_secret_key=args.s3_secret_key)
    bronze = BronzeData(schema=schema,
                        kafka_server='kafka-controller-0.kafka-controller-headless.kafka-cluster.svc.cluster.local:9092,kafka-controller-1.kafka-controller-headless.kafka-cluster.svc.cluster.local:9092,kafka-controller-2.kafka-controller-headless.kafka-cluster.svc.cluster.local:9092',
                        bucket_name='nyc-trip-bucket', spark=spark)
    bronze.csv_to_bronze(source_csv='s3a://nyc-trip-bucket/nyc-data/location.csv', target_table_name="location",
                         id_option=False)
    bronze.csv_to_bronze(source_csv='s3a://nyc-trip-bucket/nyc-data/dpc_base_num.csv',
                         target_table_name="dpc_base_num", id_option=True)
