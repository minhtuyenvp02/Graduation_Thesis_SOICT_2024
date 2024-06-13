from confluent_kafka.admin import AdminClient
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *
from minio import Minio
from datetime import datetime
from schema import CustomSchema
from spark_executor import create_spark_session
from config import SCHEMA_CONFIG
import logging
import os
import string
import uuid
import argparse


class BronzeData(object):
    def __init__(self, kafka_server: str, bucket_name: str, schema, spark: SparkSession):
        self.schema = schema
        self.bronze_location = f"s3a://{bucket_name}/bronze"
        self.kafka_server = kafka_server
        kafka_conf = {
            "bootstrap.servers": kafka_server
        }
        admin_client = AdminClient(kafka_conf)
        dic_topic = admin_client.list_topics().topics
        self.topics = [x for x in dic_topic.keys()]
        self.bucket_name = bucket_name
        self.spark = spark

    def csv_to_bronze(self, source_csv, target_table_name, id_option: bool):
        df = self.spark.read.format("csv") \
            .option("delimiter", ";") \
            .option("inferSchema", True) \
            .option("header", True) \
            .load(source_csv)
        if id_option:
            df = df.withColumn("id", monotonically_increasing_id() + 1) \
                    .drop("hv_license_num")
        df = df.withColumn("effective_date", lit(datetime.now().date()))
        df.printSchema()
        df.show()
        table_path = f"{self.bronze_location}/{target_table_name}"
        df.write \
            .format("delta") \
            .option("overwriteSchema", "true") \
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
        df.printSchema()
        stream_query = df \
            .writeStream \
            .format("delta") \
            .option("checkpointLocation", f"{target_location}/_checkpoint") \
            .start(target_location)
        stream_query.awaitTermination()

    def kafka_stream_2bronze(self, topic: str):
        # print(S3_CONFIG["fs.s3a.endpoint"])
        # client = Minio(
        #     endpoint=S3_CONFIG["fs.s3a.endpoint"],
        #     access_key=S3_CONFIG["fs.s3a.access.key"],
        #     secret_key=S3_CONFIG["fs.s3a.secret.key"],
        #     cert_check=False,
        #     secure=False
        # )
        client = Minio(
            endpoint='minio.minio.svc.cluster.local:9000',
            access_key='admin',
            secret_key='admin123',
            cert_check=False,
            secure=False
        )
        found = client.bucket_exists(self.bucket_name)
        if not found:
            logging.info(f"Bucket {self.bucket_name} doesn't exist, auto make...")
            client.make_bucket(bucket_name=self.bucket_name, )
        # print(topics)
        self.topic_2bronze(topic, os.path.join(self.bronze_location, topic))
        print("Consume Done")
