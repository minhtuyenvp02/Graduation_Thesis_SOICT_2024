# import io
# import os
#
# import pyspark
#
# from schema import get_chema
# import minio
# from delta import *
# from pyspark.sql import SparkSession, Column
# from pyspark.sql.functions import current_timestamp, from_json
#
# client = minio.Minio("10.211.56.7:30090"
#                      , access_key="admin"
#                      , secret_key="admin123"
#                      , cert_check=False
#                      , secure=False)
#
# print(client.list_buckets())
# bucket_name = "test-bucket"
# found = client.bucket_exists(bucket_name)
# print(found)
# if not found:
#     print("make")
#     client.make_bucket(bucket_name)
#
# # result = client.put_object(
# #     bucket_name=bucket_name, object_name="my-object", data=io.BytesIO(b"hello"), length=5,
# #     content_type="application/csv",
# # )
# # session = SparkSession.builder \
# #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")\
# #     .getOrCreate()
# S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "admin")
# S3_BUCKET = os.environ.get("S3_BUCKET", "test-bucket")
# S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "admin123")
# S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://10.211.56.7:30090")
# # S3_ACCESS_KEY = "sparkaccesskey"
# # S3_BUCKET = "test"
# # S3_SECRET_KEY = "sparksupersecretkey"
# # S3_ENDPOINT = "http://minio:9000"
#
# # conf = pyspark.SparkConf().setMaster("local[*]")
# # conf.set("spark.jars.packages", 'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1')
# # # conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
# # conf.set('spark.hadoop.fs.s3a.endpoint', S3_ENDPOINT)
# # conf.set('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY)
# # conf.set('spark.hadoop.fs.s3a.secret.key', S3_SECRET_KEY)
# # conf.set('spark.hadoop.fs.s3a.path.style.access', "true")
# # conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# # conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
# # conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
# # conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#
# # sc = pyspark.SparkContext(conf=conf)
# # # builder = SparkSession.builder.config(conf=conf)
# # spark = configure_spark_with_delta_pip(builder\).getOrCreate()
# # spark = SparkSession(sc)
#
#
# builder = SparkSession.builder.appName("MyApp") \
#     .config("spark.sql.shuffle.partitions", 4) \
#     .config("spark.sql.streaming.schemaInference", "true") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#
# spark_ = (configure_spark_with_delta_pip(builder, extra_packages=['org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1'])
#           .getOrCreate())
# # add confs
# sc = spark_.sparkContext
# sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
# sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "admin123")
# sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://10.211.56.7:30090")
# sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
# sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
# sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
# # spark_.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
# target_location = f"s3a://{bucket_name}/test/delta/YellowTripTest1/"
# target_location3 = f"s3a://{bucket_name}/nyc-bronze/"
# checkpoint_location = f"{target_location}_checkpoint/"
# checkpoint_location3 = f"{target_location3}_checkpoint/"
# yl_df = spark_.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.211.56.7:31463") \
#     .option("includeHeaders", True) \
#     .option("subscribe", "yellow_tripdata") \
#     .option("startingOffsets", "earliest") \
#     .load()
# json_df = yl_df.selectExpr("cast(value as string) as value")
#
# df = json_df.withColumn("value", from_json(json_df["value"], get_chema("yellow_tripdata"))).select("value.*")
#
# # df = yl_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#
# # # yl_df = yl_df.withColumn("RecordStreamTime", current_timestamp())
# #
# df.printSchema()
# print(df.isStreaming)
# # # print(yl_df.collect())
# # # print(yl_df.agg(Column("total_amount")))
# # # print(yl_df.count())
# # # print(yl_df.head(10))
# #
# query = df.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", checkpoint_location3) \
#     .start(target_location3)
# # df = yl_df.selectExpr("CAST(value AS STRING)")STRING
# # query = df.writeStream \
# #     .format("console") \
# #     .option("numRows", 1000) \
# #     .outputMode("append") \
# #     .start()
#
# query.awaitTermination()
#
#
# # df_uploaded = spark_.read.format("delta").load(target_location)
# # print(df_uploaded.count())
from stream_to_bronze import BronzeData
from schema import CustomSchema
from config import S3_CONFIG, SPARK_CONFIG, SCHEMA_CONFIG, KAFKA_CONFIG, EXTRA_JAR_PACKAGE
from spark_executor import create_spark_seesion
schema = CustomSchema(SCHEMA_CONFIG)
bronze_data = BronzeData(spark_seesion=create_spark_seesion(), kafka_server=KAFKA_CONFIG["bootstrap_servers"], bucket_name="nyc-trip-bucket", schema=schema)
bronze_data.kafka_stream_2bronze(["yellow_tripdata", "green_tripdata", "fhv_tripdata"])