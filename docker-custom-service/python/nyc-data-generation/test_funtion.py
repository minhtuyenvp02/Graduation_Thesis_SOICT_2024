import time

from confluent_kafka import Producer

from trip_data_generator import TripGenerator

generator = TripGenerator(kafka_bootstrap_server='10.211.56.7:31463', data_dir="test-bucket/nyc-data/2023", date_str=time.time(), part_idx=0, url_endpoint="http://10.211.56.7:30090")
generator.simulate_streaming()