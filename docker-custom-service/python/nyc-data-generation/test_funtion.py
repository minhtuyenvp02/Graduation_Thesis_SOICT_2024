import time

from confluent_kafka import Producer

from trip_data_generator import TripGenerator

generator = TripGenerator(kafka_bootstrap_server='10.211.56.9:30196,10.211.56.9:32476,10.211.56.9:30258', data_dir="nyc-trip-bucket/nyc-data/2023/", date_str=time.time(), part_idx=0, url_endpoint="http://api.minio.local:32589")
generator.simulate_streaming()