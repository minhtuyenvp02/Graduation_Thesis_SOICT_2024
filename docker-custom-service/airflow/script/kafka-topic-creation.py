from confluent_kafka.admin import AdminClient, NewTopic
from spark.config import *


def delete_kafka_topic(kafka_server: str, topics: [str], n_partitions: int, n_nodes: int):
    conf = {
        "bootstrap.servers": kafka_server
    }

    admin_client = AdminClient(conf)
    admin_client.delete_topics(topics)
    # for topic in topics:
    #     topic_lst = [NewTopic(topic, num_partitions=n_partitions, replication_factor=n_nodes)]
    #     admin_client.delete_topics(topic_lst)


# Initialize the kafka topic
def create_kafka_topic(kafka_servers: str, topics: [str], n_partitions=2, n_nodes=2):
    """Create the kafka topic for the kafka server"""
    conf = {
        "bootstrap.servers": kafka_servers
    }
    admin_client = AdminClient(conf)
    for topic in topics:
        topic_lst = [NewTopic(topic, num_partitions=n_partitions, replication_factor=n_nodes)]
        if topic not in admin_client.list_topics().topics:
            topic_lst = [NewTopic(topic, num_partitions=n_partitions, replication_factor=n_nodes)]
            fs = admin_client.create_topics(topic_lst)
            # fs = admin_client.delete_topics(topic_lst)
            print(fs)
            for topic, f in fs.items():
                print("still oke")
                try:
                    print("oke")
                    f.result()
                    print("oke")
                    print(f"Topic {topic} created")
                except Exception as e:
                    print(f"Failed to create topic {topic}: {e}")
        else:
            print(f"Topic {topic} is already created")


create_kafka_topic(KAFKA_CONFIG["bootstrap_servers"], ["yellow_tripdata", "fhvhv_tripdata"], 2, 2)
conf = {
    "bootstrap.servers": KAFKA_CONFIG["bootstrap_servers"]
}
admin_client = AdminClient(conf)
# admin_client.describe_topics(["fhvhv_tripdata"])
list_topic = admin_client.list_topics().topics
ls = [x for x in list_topic.keys()]
print(ls)
