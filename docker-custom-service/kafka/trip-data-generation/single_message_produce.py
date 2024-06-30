import os
import random
import time
import pyarrow.parquet as pq
import pandas as pd
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

import fsspec
import s3fs


class SingleMessageProducer(object):
    def __init__(self, producer: Producer, part_idx, send_speed=500):
        self.producer = producer
        self.part_idx = part_idx
        self.total_message = 0
        self.send_speed = send_speed  # the numbers of messages / second default 50

    def send_single_item(self, url_file_path: str, topics: [str], send_speed, s3_endpoint: str):
        fsspec.config.conf = {
            "s3":
                {
                    "key": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
                    "secret": os.getenv("AWS_SECRET_ACCESS_KEY", "admin123"),
                    "client_kwargs": {
                        "endpoint_url": f"{s3_endpoint}"
                    }
                }
        }
        s3 = s3fs.S3FileSystem()
        s3_path = s3.open("s3://" + url_file_path)
        parquet_file = pq.ParquetFile(s3_path)
        topic_name = url_file_path.split("/")[-1][0:-16]
        print("Starting send single message")
        if topics is None:
            logging.info("No topic")
            return None
        if topic_name in topics:
            print(topic_name)
            if topic_name == 'fhvhv_tripdata':
                trip_send_speed = send_speed * 4
            else:
                trip_send_speed = send_speed
            for batch in parquet_file.iter_batches():
                df = batch.to_pandas()
                for index, row in df.iterrows():
                    try:
                        print(f"Publishing to topic {topic_name}")
                        self.producer.produce(topic=topic_name, value=bytes(row.to_json(), 'utf-8'))
                        time_random = 1.0 / trip_send_speed
                        time_sleep = random.uniform(time_random, time_random + time_random / 2)
                        time.sleep(time_sleep)
                        self.producer.flush()
                    except Exception as e:
                        logging.info(e)
        else:
            logging.info(f"Topic {topic_name} is not valid.")
