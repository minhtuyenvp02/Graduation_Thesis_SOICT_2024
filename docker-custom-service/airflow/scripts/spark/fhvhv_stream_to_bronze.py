from pyspark.sql import SparkSession
from bronze_medallion import BronzeData
from config import *
from spark_executor import create_spark_session
from datetime import datetime
from schema import CustomSchema
if __name__ == "__main__":
    print(datetime.now())
    schema = CustomSchema(SCHEMA_CONFIG)
    spark = create_spark_session(app_name="Kafka Stream To Bronze", spark_cluster=args.spark_cluster,
                                 s3_endpoint=args.s3_endpoint, s3_access_key=args.s3_access_key,
                                 s3_secret_key=args.s3_secret_key)
    bronze = BronzeData(schema=schema, kafka_server=args.kafka_servers, bucket_name=args.bucket_name, spark=spark
                        )
    bronze.kafka_stream_2bronze("fhvhv_tripdata")