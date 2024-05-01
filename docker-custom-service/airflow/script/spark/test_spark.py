# import  pandas as pd
# import minio
# from pyspark.sql import SparkSession
# from delta import *
# client = minio.Minio("192.168.1.13:30090"
#                ,access_key="admin"
#                ,secret_key="admin123")
# bucket_name = "test_bucket"
# found = client.bucket_exists(bucket_name)
# if not found:
#     client.make_bucket(bucket_name)
#
# builder = SparkSession.builder.appName("MyApp") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#
# spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()
#
# # add confs
# sc = spark.sparkContext
# sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
# sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "admin123")
# sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://192.168.1.16:30090")
# sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
# sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
# sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
#
#
# yellow_df = spark.readStream.format("parquet").option("header", "true").option("inferSchema", "true").load("/Users/minhtuyen02/MTuyen/work-space/University/DATN_2024/DATN_2024_Gitlab/graduate-project/yellow_tripdata_2023-01.parquet")
#
# yellow_df.writeStream.format("delta").outputMode("append").start(f"s3a://{bucket_name}/test/delta")
