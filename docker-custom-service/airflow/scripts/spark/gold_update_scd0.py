from spark.core_dwh import *
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import logging
from spark.spark_executor import create_spark_session
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark_cluster", type=str, required=True)
    parser.add_argument("--bucket_name", type=str, required=True)
    parser.add_argument("--s3_endpoint", type=str, required=True)
    parser.add_argument("--s3_access_key", type=str, required=True)
    parser.add_argument("--s3_secret_key", type=str, required=True)
    args = parser.parse_args()
    spark = create_spark_session(app_name="Gold Create SCD1", spark_cluster=args.spark_cluster,
                                 s3_endpoint=args.s3_endpoint, s3_access_key=args.s3_access_key,
                                 s3_secret_key=args.s3_secret_key)
    silver_location = f"s3a://{args.bucket_name}/silver"
    gold_location = f"s3a://{args.bucket_name}/gold"
    builder = WareHouseBuilder(silver_location=silver_location, dwh_location=gold_location, spark=spark)
    builder.run_dim_builder()
