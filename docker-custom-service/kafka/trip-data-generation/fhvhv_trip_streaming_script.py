import argparse
import sys
from trip_data_generator import TripGenerator


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka_servers", type=str, required=True)
    parser.add_argument("--data_dir", type=str, required=True)
    parser.add_argument("--send_speed", type=str, required=True)
    parser.add_argument("--minio_endpoint", type=str, required=True)
    args = parser.parse_args()
    generator = TripGenerator(
        kafka_bootstrap_server=args.kafka_servers,
        data_dir=args.data_dir,
        send_speed=int(args.send_speed),
        minio_endpoint=args.minio_endpoint,
    )
if __name__ == '__main__':
    main()
