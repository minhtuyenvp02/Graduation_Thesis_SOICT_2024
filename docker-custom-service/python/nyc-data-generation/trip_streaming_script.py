import argparse
import sys
from trip_data_generator import TripGenerator


def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--kafka_server", type=str, required=True)
    # parser.add_argument("--data_dir", type=str, required=True)
    # parser.add_argument("--date_str", type=str, required=True)
    # parser.add_argument("--send_speed", type=str, required=True)
    # parser.add_argument("--url_endpoint", type=str, required=True)
    # args = parser.parse_args()
    generator = TripGenerator(
        kafka_bootstrap_server='10.211.56.3:32567,10.211.56.3:32285,10.211.56.3:30970',
        data_dir='nyc-trip-bucket/nyc-data/2023/',
        send_speed=int(200),
        url_endpoint='http://10.211.56.3:30090',
    )
    generator.simulate_streaming()


if __name__ == '__main__':
    main()
