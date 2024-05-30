import os
import random
import time

import pandas as pd
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer


class SingleMessageProducer(object):
    def __init__(self, producer: Producer, part_idx, send_speed=500):
        self.producer = producer
        self.part_idx = part_idx
        self.total_message = 0
        self.send_speed = send_speed  # the numbers of messages / second default 50

    def acked_calback(self, error, message):
        """Callback for message delivery reports."""
        if error is not None:
            print(f"Failed to deliver message: {error}")
        else:
            print(f"Message produced: {message.topic()} [{message.partition()}] @ {message.offset()}")

    def send_single_item(self, url_file_path: str, topics: [str]):
        # print(" o day")
        df = pd.read_parquet(path="s3://" + url_file_path, storage_options={"anon": False})
        topic_name = url_file_path.split("/")[-1][0:-16]
        if topics is None:
            logging("No topic")
            return None
        if topic_name in topics:
            for index, row in df.iterrows():
                # print(index)
                try:
                    print(f"Publishing to topic {topic_name}")
                    self.producer.produce(topic=topic_name, value=bytes(row.to_json(), 'utf-8'))
                    # self.producer.produce(topic="test", value=bytes(row.to_json(), 'utf-8'),
                    #                       partition=self.part_idx, callback=self.acked_calback())
                    time_random = 1 / self.send_speed
                    time_sleep = random.uniform(time_random, time_random + time_random/2)
                    print(time_sleep)
                    time.sleep(time_sleep)
                    self.producer.flush()
                except Exception as e:
                    logging.info(e)
        else:
            logging.info(f"Topic {topic_name} is not valid.")
