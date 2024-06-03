import argparse
import sys
sys.path.append("/opt/airflow/scripts/trip_data_generation/")
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
    # generator = TripGenerator(
    #     kafka_bootstrap_server="10.211.56.3:31723,10.211.56.3:32120,10.211.56.3:32745",
    #     data_dir="nyc-trip-bucket/nyc-data/2023",
    #     send_speed=200,
    #     minio_endpoint="http://10.211.56.3:30090",
    # )
    generator.simulate_streaming()


if __name__ == '__main__':
    main()
