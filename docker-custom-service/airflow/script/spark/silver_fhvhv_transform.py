import logging
from pyspark.sql import SparkSession
from silver_medallion import Silver
from config import *
from delta import configure_spark_with_delta_pip
from schema import CustomSchema
from spark_executor import create_spark_session
if __name__ == "__main__":
    spark = create_spark_session(app_name="SilverFhvTransform")
    silver = Silver(bucket_name="nyc-trip-bucket", schema=CustomSchema(SCHEMA_CONFIG), spark=spark)
    silver.fhvhv_transform()
