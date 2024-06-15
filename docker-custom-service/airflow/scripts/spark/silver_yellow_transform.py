from silver_medallion import Silver
from spark_executor import create_spark_session
import argparse
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', "http://minio.minio.svc.cluster.local:9000")
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY', "admin")
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY', "admin123")
KAFKA_CONSUMER_SERVER = os.environ.get('KAFKA_CONSUMER_SERVER', "kafka.kafka.svc.cluster.local:9092")
BUCKET_NAME = os.environ.get('BUCKET_NAME', "nyc-trip-bucket")

if __name__ == "__main__":
    spark = create_spark_session(app_name="Silver Yellow Transform",
                                 s3_endpoint=S3_ENDPOINT, s3_access_key=S3_ACCESS_KEY,
                                 s3_secret_key=S3_SECRET_KEY)
    silver = Silver(bucket_name=BUCKET_NAME, spark=spark)
    silver.yellow_transform()