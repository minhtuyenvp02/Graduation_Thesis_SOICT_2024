from concurrent.futures import ThreadPoolExecutor

from single_message_produce import SingleMessageProducer
import os
import sys
import fsspec
import s3fs
import logging
from confluent_kafka import Producer

sys.path.append("opt/airflow/script/stock_data_generation/")

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO, force=True)


class TripGenerator(object):
    def __init__(self, kafka_bootstrap_server: str, topics: [str], url_endpoint: str, data_dir: str, part_idx: int,
                 date_str: str):
        self.topics = topics
        self.kafka_servers = kafka_bootstrap_server
        self.url_endpoint = url_endpoint
        self.data_dir = data_dir
        config_ = {
            "bootstrap.servers": self.kafka_servers
        }
        producer = Producer(config_)

        self.generator = SingleMessageProducer(part_idx=0, producer=producer)

    def simulate_streaming(self):
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
        for i, dir_path in enumerate(list_dir):
            try:
                print(dir_path)
                list_file = s3.find(dir_path["name"])
            except Exception as e:
                logging.info(e)
            if len(list_file) == 0:
                logging.info("This directory has no file")
            else:
                try:
                    with ThreadPoolExecutor(max_workers=5) as execute:
                        task_result = [False] * 4
                        for idx, file in enumerate(list_file):
                            try:
                                print("err___01")
                                task_result[idx] = execute.submit(self.generator.send_single_item, file, self.topics)
                                print("error_02")
                                print(task_result[idx])
                            except Exception as e:
                                logging.info("Submmit task failed")
                except Exception as e:
                    # print("errr")
                    logging.info("Error when create threads")
