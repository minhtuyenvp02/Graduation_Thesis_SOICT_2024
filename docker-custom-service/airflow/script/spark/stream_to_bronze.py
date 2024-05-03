import logging
import os
import string
from concurrent.futures import ThreadPoolExecutor

from confluent_kafka.admin import AdminClient

from config import S3_CONFIG
from minio import Minio
from pyspark.sql.functions import from_json


class BronzeData(object):
    def __init__(self, spark_seesion, kafka_server: str
                 , bucket_name: str, schema):
        self.spark = spark_seesion
        self.schema = schema
        self.kafka_server = kafka_server
        kafka_conf = {
            "bootstrap.servers": "10.211.56.7:31463"
        }
        admin_client = AdminClient(kafka_conf)
        dic_topic = admin_client.list_topics().topics
        self.topics = [x for x in dic_topic.keys()]
        self.bucket_name = bucket_name

    def topic_2Bronze(self, topic: str, target_location: str):
        print(f"Submit at topic {topic}")
        raw_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
        raw_json_df = raw_df.selectExpr("cast(value as string) as value")

        df = raw_json_df.withColumn("value", from_json(raw_json_df["value"], self.schema.get_schema(topic))).select(
            "value.*")
        df.printSchema()
        stream_query = df.writeStream \
            .format("delta") \
            .option("checkpointLocation", f"{target_location}/_checkpoint") \
            .start(target_location)
            # .trigger(availableNow=True) \
            # .option("checkpointLocation", f"{target_location}/_checkpoint") \
            # .start(target_location)

        stream_query.awaitTermination()

    def kafka_stream_2bronze(self, topics: [str]):
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
        base_path = f"s3a://{self.bucket_name}/bronze"
        print(base_path)
        print(topics)
        try:
            with ThreadPoolExecutor(max_workers=5) as execute:
                task_result = [False] * 4
                for idx, topic in enumerate(topics):
                    print(f"{idx}")
                    print(f"Consume from topic {topic}")
                    try:
                        print(os.path.join(base_path, topic.upper()))
                        task_result[idx] = execute.submit(
                            self.topic_2Bronze, topic, os.path.join(base_path, topic.upper()))
                        print(task_result[idx])
                    except Exception as e:
                        logging.info("Submmit task failed")
        except Exception as e:
            # print("errr")
            logging.info("Error when create threads")
