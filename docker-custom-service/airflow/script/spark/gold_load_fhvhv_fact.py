from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from config import *
import logging
from spark_executor import create_spark_session
from gold_medallion import Gold
   
if __name__ == "__main__":
    spark = create_spark_session(app_name="GoldIngestToFhvFact")
    gold = Gold(bucket_name="nyc-trip-bucket", spark=spark)
    try:
        gold.update_fact_fhvhv_trip()
    except Exception as E:
        logging.info("Failed to load data to gold")
        logging.info(E)
