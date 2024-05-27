import logging
import sys
from core_dwh import WareHouseBuilder
from gold_medallion import Gold
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *
from config import *
import logging


def run_create_dimension_tbl():
    builder = SparkSession.builder.appName("SilverYellowTransform") \
        .config("master", "local[2]") \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.optimize.repartition.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
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

    dwh_builder = WareHouseBuilder(dwh_location="s3a://nyc-trip-bucket/gold",
                                   silver_location="s3a://nyc-trip-bucket/silver", spark=spark)
    try:
        dwh_builder.run_dim_builder()
    except Exception as E:
        logging.info("FAILED to create dim")
        logging.info(E)


def update_dim_location():
    builder = SparkSession.builder.appName("SilverYellowTransform") \
        .config("master", "local[2]") \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.optimize.repartition.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
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

    gold = Gold(bucket_name="nyc-trip-data", spark=spark)
    glod.update_gold_location()


def update_dim_location():
    builder = SparkSession.builder.appName("SilverYellowTransform") \
        .config("master", "local[2]") \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.optimize.repartition.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
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

    gold = Gold(bucket_name="nyc-trip-data", spark=spark)
    glod.update_gold_location()


def update_dim_dpc_base_num():
    builder = SparkSession.builder.appName("SilverYellowTransform") \
        .config("master", "local[2]") \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.optimize.repartition.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
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

    gold = Gold(bucket_name="nyc-trip-data", spark=spark)
    glod.update_dpc_base_num()


def update_fact_yellow_trip():
    builder = SparkSession.builder.appName("SilverYellowTransform") \
        .config("master", "local[2]") \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.optimize.repartition.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
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

    gold = Gold(bucket_name="nyc-trip-data", spark=spark)
    glod.update_fact_yellow_trip()


def update_fact_fhvhv_trip():
    builder = SparkSession.builder.appName("SilverYellowTransform") \
        .config("master", "local[2]") \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.optimize.repartition.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
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

    gold = Gold(bucket_name="nyc-trip-data", spark=spark)
    glod.update_fact_fhvhv_trip()


def update_fact_fhvhv_tracking_location_daily():
    builder = SparkSession.builder.appName("SilverYellowTransform") \
        .config("master", "local[2]") \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.optimize.repartition.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
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

    gold = Gold(bucket_name="nyc-trip-data", spark=spark)
    glod.update_fact_fhvhv_tracking_location_daily()


def update_fact_yellow_tracking_location_daily():
    builder = SparkSession.builder.appName("SilverYellowTransform") \
        .config("master", "local[2]") \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.optimize.repartition.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
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

    gold = Gold(bucket_name="nyc-trip-data", spark=spark)
    glod.update_fact_yellow_tracking_location_daily()
