import argparse
import sys
from trip_data_generator import TripGenerator


def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--kafka_servers", type=str, required=True)
    # parser.add_argument("--data_dir", type=str, required=True)
    # parser.add_argument("--send_speed", type=str, required=True)
    # parser.add_argument("--minio_endpoint", type=str, required=True)
    # args = parser.parse_args()
    # generator = TripGenerator(
    #     kafka_bootstrap_server=args.kafka_servers,
    #     data_dir=args.data_dir,
    #     send_speed=int(args.send_speed),
    #     minio_endpoint=args.minio_endpoint,
    # )
    generator = TripGenerator(
        kafka_bootstrap_server="10.211.56.3:31044,10.211.56.3:31168,10.211.56.3:30351",
        data_dir='s3a://nyc-trip-bucket/nyc-data',
        send_speed=int(500),
        minio_endpoint='http://10.211.56.3:30090',
    )
    generator.simulate_streaming('fhvhv_tripdata')


if __name__ == '__main__':
    main()
