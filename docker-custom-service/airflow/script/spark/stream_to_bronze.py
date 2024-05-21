import logging
import os
import string
import uuid
import argparse
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.functions import *
from confluent_kafka.admin import AdminClient
from pyspark.sql import SparkSession
from delta import *
from minio import Minio
from schema import CustomSchema
from config import S3_CONFIG, SPARK_CONFIG, SCHEMA_CONFIG, KAFKA_CONFIG, EXTRA_JAR_PACKAGE


class BronzeData(object):
    def __init__(self, kafka_server: str, spark_cluster: str
                 , bucket_name: str, schema):
        self.schema = schema
        self.spark_cluster = spark_cluster
        self.bronze_location = f"s3a://{bucket_name}/bronze"
        self.kafka_server = kafka_server
        kafka_conf = {
            "bootstrap.servers": kafka_server
        }
        admin_client = AdminClient(kafka_conf)
        dic_topic = admin_client.list_topics().topics
        self.topics = [x for x in dic_topic.keys()]
        self.bucket_name = bucket_name

        builder = SparkSession.builder.appName("Bronze") \
            .config("master", "local[2]") \
            .config("spark.sql.shuffle.partitions", 4) \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
        self.spark = (configure_spark_with_delta_pip(builder, extra_packages=[
            'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1'])
                      .getOrCreate())
        self.spark.conf.set("spark.debug.maxToStringFields", 100)
        # add confs
        sc = self.spark.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", S3_CONFIG["fs.s3a.access.key"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", S3_CONFIG["fs.s3a.secret.key"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", S3_CONFIG["fs.s3a.endpoint"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    def csv_to_bronze(self, source_csv, target_table_name, id_option: bool):
        df = self.spark.read.format("csv") \
            .option("delimiter", ";") \
            .option("inferSchema", True) \
            .option("header", True) \
            .load(source_csv)
        if id_option:
            df = df.withColumn("id", monotonically_increasing_id() + 1)
        df.printSchema()
        df.show()
        table_path = f"{self.bronze_location}/{target_table_name}"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option('path', table_path) \
            .saveAsTable(target_table_name)

    def topic_2bronze(self, topic: str, target_location: str):
        print(f"Submit at topic {topic}")
        raw_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server) \
            .option("subscribe", topic) \
            .option("failOnDataLoss", "false") \
            .option("startingOffsets", "earliest") \
            .load()
        raw_json_df = raw_df.selectExpr("cast(value as string) as value")

        df = (raw_json_df.withColumn("value", from_json(raw_json_df["value"], self.schema.get_schema(topic)))
              .select("value.*"))
        # df = df.withColumn("id", monotonically_increasing_id())
        df.printSchema()
        stream_query = df.writeStream \
            .format("delta") \
            .option("ignoreChanges", 'true') \
            .option("checkpointLocation", f"{target_location}/_checkpoint") \
            .start(target_location)
        stream_query.awaitTermination()

    def kafka_stream_2bronze(self, topics: [str]):
        print(S3_CONFIG["fs.s3a.endpoint"])
        client = Minio(
            endpoint=S3_CONFIG["fs.s3a.endpoint"],
            access_key=S3_CONFIG["fs.s3a.access.key"],
            secret_key=S3_CONFIG["fs.s3a.secret.key"],
            cert_check=False,
            secure=False
        )
        found = client.bucket_exists(self.bucket_name)
        if not found:
            logging.info(f"Bucket {self.bucket_name} doesn't exist, auto make...")
            client.make_bucket(bucket_name=self.bucket_name, )
        print(topics)
        try:
            with ThreadPoolExecutor(max_workers=3) as execute:
                task_result = [False] * 2
                for idx, topic in enumerate(topics):
                    print(f"{idx}")
                    print(f"Consume from topic {topic}")
                    try:
                        task_result[idx] = execute.submit(
                            self.topic_2bronze, topic, os.path.join(self.bronze_location, topic))
                        print(task_result[idx])
                    except Exception as e:
                        logging.info("Submmit task failed")
        except Exception as e:
            logging.info("Error when create threads")


if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--spark_cluster", type=str, required=True)
    # parser.add_argument("--kafka_server", type=str, required=True)
    # parser.add_argument("--topics", type=str, required=True)
    # parser.add_argument("--bucket_name", type=str, required=True)
    # parser.add_argument("--schema", type=str, required=True)
    # parser.add_argument("--location_csv", type=str, required=True)
    # parser.add_argument("--fhvhv_basenum_csv", type=str, required=True)
    # args = parser.parse_args()
    schema = CustomSchema(SCHEMA_CONFIG)
    bronze = BronzeData(schema=schema, kafka_server="10.211.56.9:30196,10.211.56.9:32476,10.211.56.9:30258", spark_cluster="local[2]", bucket_name="nyc-trip-bucket")
    bronze.csv_to_bronze(source_csv="s3a://nyc-trip-bucket/nyc-data/location.csv", target_table_name="location", id_option=False)
    bronze.csv_to_bronze(source_csv="s3a://nyc-trip-bucket/nyc-data/dpc_base_num_fn.csv", target_table_name="dpc_base_num", id_option=True)
    bronze.kafka_stream_2bronze(topics=["yellow_tripdata", "fhvhv_tripdata"])
