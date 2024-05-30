import logging
from confluent_kafka.admin import AdminClient, NewTopic
from spark.config import *


# Initialize the kafka topic
def create_kafka_topic(kafka_servers: str, topics: [str], n_partitions=2, n_nodes=2):
    """Create the kafka topic for the kafka server"""
    conf = {
        "bootstrap.servers": kafka_servers
    }
    admin_client = AdminClient(conf)
    list_topic = admin_client.list_topics().topics
    ls = [x for x in list_topic.keys()]
    print(ls)

    for topic in topics:
        if topic not in admin_client.list_topics().topics:
            topic_lst = [NewTopic(topic, num_partitions=n_partitions, replication_factor=n_nodes)]
            fs = admin_client.create_topics(topic_lst)
            for topic, f in fs.items():
                try:
                    logging.info(f"Topic {topic} created")
                except Exception as e:
                    logging.info(f"Failed to create topic {topic}: {e}")
        else:
            logging.info(f"Topic {topic} is already created")
