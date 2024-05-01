from spark_executor import SparkExecutor
from delta import *
# from delta import configure_spark_with_delta_pip

class BronzeData(object):
    def __init__(self, spark_executor: SparkExecutor, kafka_server: str, kafka_topic: str, path_to_bronze: str):
        self.spark = spark_executor.create_spark_session()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.kafka_server = kafka_server
        self.kafka_topic = kafka_topic
        self.bronze_layer = path_to_bronze

    def kafka_stream_to_s3(self, topic_name):

        raw_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load() \
            .writeStream\
            .format("delta") \
            .outputMode("append")








    # def main(self):




# if __name__ == '__main__':
