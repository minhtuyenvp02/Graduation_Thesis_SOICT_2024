from gold_medallion import Gold
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from config import *
import logging
from spark_executor import create_spark_session


if __name__ == "__main__":
    spark = create_spark_session(app_name="UpdateSCD2")
    gold = Gold(bucket_name="nyc-trip-bucket", spark=spark)
    gold.update_gold_location()
    gold.update_dpc_base_num()
