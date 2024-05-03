import argparse
import sys
from trip_data_generator import TripGenerator
sys.path.append("/opt/airflow/scripts/stock_data_generation/")


def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--kafka_bootstrap_server", type=str, required=True)
    # parser.add_argument("--url_endpoint", type=str, required=True)
    # parser.add_argument("--topics", type=[str], required=True)
    # parser.add_argument("--path_dir", type=str, required=True)
    # parser.add_argument("--date_str", type=str, required=True)
    # parser.add_argument("--part_idx", type=str, required=True)
    # args = parser.parse_args()
    boot_strap_server = ["kafka-controller-0.kafka-controller-headless.kafka-cluster.svc.cluster.local:9092"
    ,"kafka-controller-1.kafka-controller-headless.kafka-cluster.svc.cluster.local:9092"
    ,"kafka-controller-2.kafka-controller-headless.kafka-cluster.svc.cluster.local:9092"]
    trip_data_generator = TripGenerator(topics=["yellow_tripdata", "fhv_tripdata", "fhvhv_tripdata", "green_tripdata"], kafka_bootstrap_server="")



if __name__ == "__main__":
    main()
