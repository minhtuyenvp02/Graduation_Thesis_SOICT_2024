from gold_medallion import Gold
from config import *
import logging
from spark_executor import create_spark_session
import argparse


if __name__ == "__main__":
    spark = create_spark_session(app_name="Gold Update SCD2",
                                 s3_endpoint="http://minio.minio.svc.cluster.local:9000", s3_access_key='admin',
                                 s3_secret_key='admin123')
    gold = Gold(bucket_name="nyc-trip-bucket", spark=spark)
    gold.update_gold_location()
    gold.update_dpc_base_num()
