from main_gold import *
from core_dwh import *
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import logging
from spark_executor import create_spark_session


if __name__ == "__main__":
    spark = create_spark_session(app_name="CreateDimensions")
    builder = WareHouseBuilder(silver_location="s3a://nyc-trip-bucket/silver", dwh_location="s3a://nyc-trip-bucket/gold", spark=spark)
    builder.run_dim_builder()
