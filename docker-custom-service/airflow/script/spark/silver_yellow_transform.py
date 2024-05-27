import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from silver_medallion import Silver
from config import SCHEMA_CONFIG
from schema import CustomSchema
from config import *
from spark_executor import create_spark_session

if __name__ == "__main__":
    spark = create_spark_session(app_name="SilverYellowTransform")
    silver = Silver(bucket_name="nyc-trip-bucket", schema=CustomSchema(SCHEMA_CONFIG), spark=spark)
    # silver.create_yellow_streaming_table()
    silver.yellow_transform()