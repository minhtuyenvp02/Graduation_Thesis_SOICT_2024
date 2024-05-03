from confluent_kafka.admin import AdminClient, NewTopic


# Initialize the kafka topic
def create_kafka_topic(kafka_servers: str, topics: [str], n_partitions: int, n_nodes=1):
    """Create the kafka topic for the kafka server"""
    conf = {
        "bootstrap.servers": kafka_servers
    }
    admin_client = AdminClient(conf)
    for topic in topics:
        if topic not in admin_client.list_topics().topics:
            topic_lst = [NewTopic(topic, num_partitions=n_partitions, replication_factor=n_nodes)]
            fs = admin_client.create_topics(topic_lst)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"Topic {topic} created")
                except Exception as e:
                    print(f"Failed to create topic {topic}: {e}")
        else:
            print(f"Topic {topic} is already created")


create_kafka_topic("10.211.56.7:31463", ["yellow_tripdata", "green_tripdata", "fhv_tripdata", "fhvhv_tripdata"], 2, 2)
conf = {
    "bootstrap.servers": "10.211.56.7:31463"
}
# admin_client = AdminClient(conf)
# list_topic = admin_client.list_topics().topics
# ls = [x for x in list_topic.keys()]
# print(ls)
