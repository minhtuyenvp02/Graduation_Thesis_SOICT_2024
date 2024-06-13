from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from config import *
import logging
from spark_executor import create_spark_session
from gold_medallion import Gold


def main():
    spark = create_spark_session(app_name="Gold Update FHV Tracking Fact",
                                 s3_endpoint="http://minio.minio.svc.cluster.local:9000", s3_access_key='admin',
                                 s3_secret_key='admin123')
    gold = Gold(bucket_name='nyc-trip-bucket', spark=spark)
    try:
        gold.update_fact_fhvhv_tracking_location_daily()
    except Exception as E:
        logging.info("Failed to load data to gold")
        logging.info(E)


if __name__ == __main__:
    main()
