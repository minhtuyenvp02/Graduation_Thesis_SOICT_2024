import logging
from pyspark.sql import SparkSession
from config import *
from delta import configure_spark_with_delta_pip
from schema import CustomSchema


def create_spark_session(app_name: str, spark_cluster: str, s3_endpoint: str, s3_access_key: str, s3_secret_key: str):
    builder = SparkSession.builder.appName(f"{app_name}") \
        .config("master", f"{spark_cluster}") \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    spark = (configure_spark_with_delta_pip(builder, extra_packages=[
        'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0'])
             .getOrCreate())
    # add confs
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", f"{s3_access_key}")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", f"{s3_secret_key}")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"{s3_endpoint}")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    return spark
