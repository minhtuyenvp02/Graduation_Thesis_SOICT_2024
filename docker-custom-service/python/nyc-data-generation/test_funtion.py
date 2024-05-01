import time

from confluent_kafka import Producer

from trip_data_generator import TripGenerator

generator = TripGenerator(kafka_bootstrap_server='192.168.1.16:30638', data_dir="test-bucket/nyc-data/2023", date_str=time.time(), part_idx=0, topics=["test"], url_endpoint="http://192.168.0.105:30098")
generator.simulate_streaming()