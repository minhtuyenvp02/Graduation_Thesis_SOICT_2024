from pyspark.sql import SparkSession
from bronze_medallion import BronzeData
from config import *
from spark_executor import create_spark_session
from schema import CustomSchema
if __name__ == "__main__":
    schema = CustomSchema(SCHEMA_CONFIG)
    spark = create_spark_session(app_name="Yellow Trip To Bronze",
                                 s3_endpoint="http://minio.minio.svc.cluster.local:9000", s3_access_key='admin',
                                 s3_secret_key='admin123')
    bronze = BronzeData(schema=schema, kafka_server='kafka.kafka.svc.cluster.local:9092', bucket_name='nyc-trip-bucket', spark=spark
                        )
    bronze.kafka_stream_2bronze(topic='yellow_tripdata')