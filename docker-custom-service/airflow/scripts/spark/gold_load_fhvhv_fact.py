from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from spark.config import *
import logging
from spark.spark_executor import create_spark_session
from spark.gold_medallion import Gold
import argparse
   
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark_cluster", type=str, required=True)
    parser.add_argument("--bucket_name", type=str, required=True)
    parser.add_argument("--s3_endpoint", type=str, required=True)
    parser.add_argument("--s3_access_key", type=str, required=True)
    parser.add_argument("--s3_secret_key", type=str, required=True)
    args = parser.parse_args()

    spark = create_spark_session(app_name="Gold Update FHV Fact", spark_cluster=args.spark_cluster,
                                 s3_endpoint=args.s3_endpoint, s3_access_key=args.s3_access_key,
                                 s3_secret_key=args.s3_secret_key)
    gold = Gold(bucket_name=args.bucket_name, spark=spark)
    try:
        gold.update_fact_fhvhv_trip()
    except Exception as E:
        logging.info("Failed to load data to gold")
        logging.info(E)
