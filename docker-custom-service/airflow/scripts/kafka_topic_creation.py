import logging
from confluent_kafka.admin import AdminClient, NewTopic


def create_kafka_topic(kafka_servers: str, topics: [str], n_partitions=3, n_nodes=2):
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
            topic_lst = [NewTopic(topic=topic, num_partitions=n_partitions, replication_factor=n_nodes,
                                  config={'retention.ms': '21600000'})]
            fs = admin_client.create_topics(topic_lst)
            for tp, f in fs.items():
                try:
                    f.result()
                    print(f"Topic {tp} created")
                except Exception as e:
                    print(f"Failed to create topic {topic}: {e}")
        else:
            print(f"Topic {topic} is already created")
