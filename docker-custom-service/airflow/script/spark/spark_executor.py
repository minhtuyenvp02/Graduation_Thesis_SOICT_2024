import logging
from pyspark.sql import SparkSession
from silver_medallion import Silver
from config import *
from delta import configure_spark_with_delta_pip
from schema import CustomSchema


def create_spark_session(app_name: str):
    builder = SparkSession.builder.appName(f"{app_name}") \
        .config("master", "local[2]") \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    spark = (configure_spark_with_delta_pip(builder, extra_packages=[
        'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1'])
             .getOrCreate())
    # add confs
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", S3_CONFIG["fs.s3a.access.key"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", S3_CONFIG["fs.s3a.secret.key"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", S3_CONFIG["fs.s3a.endpoint"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    return spark
