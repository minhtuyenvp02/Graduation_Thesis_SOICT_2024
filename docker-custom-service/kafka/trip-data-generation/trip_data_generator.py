from confluent_kafka.admin import AdminClient
from single_message_produce import SingleMessageProducer
import os
import sys
import fsspec
import s3fs
import logging
from confluent_kafka import Producer

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO, force=True)


class TripGenerator(object):
    def __init__(self, kafka_bootstrap_server: str, minio_endpoint: str
                 , data_dir: str, send_speed: int):
        self.kafka_servers = kafka_bootstrap_server
        self.url_endpoint = minio_endpoint
        self.data_dir = data_dir
        self.send_speed = send_speed
        config_ = {
            "bootstrap.servers": self.kafka_servers,
            'batch.size': 16384,
            'linger.ms': 5,
            'compression.type': 'gzip',
            'acks': 'all'
        }
        producer = Producer(**config_)
        self.generator = SingleMessageProducer(part_idx=0, producer=producer)

    def simulate_streaming(self, file_name: str):
        fsspec.config.conf = {
            "s3":
                {
                    "key": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
                    "secret": os.getenv("AWS_SECRET_ACCESS_KEY", "admin123"),
                    "client_kwargs": {
                        "endpoint_url": self.url_endpoint
                    }
                }
        }
        s3 = s3fs.S3FileSystem()
        list_dir = s3.listdir(self.data_dir)
        print(list_dir[0])
        conf = {
            "bootstrap.servers": self.kafka_servers
        }
        admin_client = AdminClient(conf)
        dic_topic = admin_client.list_topics().topics
        topics = [x for x in dic_topic.keys()]
        for i, dir_path in enumerate(list_dir):
            try:
                list_file = s3.find(dir_path["name"])
                [print(x) for x in list_file]
            except Exception as e:
                logging.info("List dir erorr")
            if len(list_file) == 0:
                logging.info("This directory has no file")
            else:
                try:
                    for idx, file in enumerate(list_file):
                        if file_name in file:
                            print(f"topic ís {file_name}")
                            self.generator.send_single_item(file, topics, send_speed=self.send_speed, s3_endpoint=self.url_endpoint)
                            print("send erorr")

                except Exception as e:
                    logging.info("Error when create threads")



