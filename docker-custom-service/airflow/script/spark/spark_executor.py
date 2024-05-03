from pyspark.sql import SparkSession
from delta import *
from config import S3_CONFIG, SPARK_CONFIG


def create_spark_seesion():
    builder = SparkSession.builder.appName("MyApp") \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
