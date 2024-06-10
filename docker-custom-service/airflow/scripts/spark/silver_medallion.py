from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from delta import *
from config import S3_CONFIG, SPARK_CONFIG
from pyspark.sql.types import *
from delta import DeltaTable
import uuid
import argparse


def generate_uuid():
    return str(uuid.uuid4())


class Silver(object):
    def __init__(self, bucket_name: str, spark: SparkSession):
        self.bucket_name = bucket_name
        self.silver_location = f"s3a://{self.bucket_name}/silver"
        self.spark = spark
        self.yellow_trip_tbl = f"{self.silver_location}/yellow_trip"
        self.fhvhv_trip_tbl = f"{self.silver_location}/fhvhv_trip"

    def create_yellow_streaming_table(self):
        self.spark.sql("CREATE DATABASE IF NOT EXISTS silver")
        self.spark.sql(" USE silver")
        self.spark.sql(f"""
             CREATE TABLE IF NOT EXISTS yellow_trip
             USING DELTA
             LOCATION '{self.yellow_trip_tbl}'
         """)
        self.spark.sql("ALTER TABLE yellow_trip SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    # def create_fhvhv_streaming_table(self):
    #     self.spark.sql(f"""
    #         CREATE DATABASE IF NOT EXISTS silver;
    #         CREATE TABLE IF NOT EXISTS silver.fhvhv_trip
    #         USING DELTA
    #         LOCATION '{self.yellow_trip_tbl}'
    #         ALTER TABLE silver.fhvhv_trip SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    #     """)

    def fhvhv_transform(self):
        time_tracking = self.spark.range(1) \
            .selectExpr("current_timestamp() - INTERVAL 1 HOURS as start_time") \
            .collect()[0]['start_time']
        df = self.spark.readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", time_tracking) \
            .load(f"s3a://{self.bucket_name}/bronze/fhvhv_tripdata")

        df = df.fillna("N", ["access_a_ride_flag"]) \
            .fillna("0.0", ["airport_fee"]) \
            .fillna("unknown", ["originating_base_num"])
        uuid_udf = udf(generate_uuid, StringType())
        df = df.withColumn("id", uuid_udf()) \
            .withColumn("total_surcharge", (df["tolls"] + df["bcf"]
                                            + df["sales_tax"] + df["congestion_surcharge"] + df["airport_fee"] + df[
                                                "tips"]).cast("decimal(10,2)"))

        df = df.withColumnRenamed("PULocationID", "pickup_location_id") \
            .withColumnRenamed("DOLocationID", "dropoff_location_id") \
            .withColumnRenamed("hvfhs_license_num", "license_num_id") \
            .withColumnRenamed("dispatching_base_num", "base_num_id") \
            .withColumn("pickup_datetime",
                        from_unixtime((df["pickup_datetime"].cast('bigint') / 1000)).cast('timestamp')) \
            .withColumn("dropoff_datetime",
                        from_unixtime((df["dropoff_datetime"].cast('bigint') / 1000)).cast('timestamp')) \
            .withColumn("totals_amount", (df["base_passenger_fare"] + df['total_surcharge']).cast("decimal(10,2)"))

        df = df.withColumn("pickup_date_id",
                           date_format(df["pickup_datetime"], 'yyyyMMdd')) \
            .withColumn("dropoff_date_id",
                        date_format(df["dropoff_datetime"], 'yyyyMMdd')) \
            .withColumn("avg_speed", when(df["trip_time"] == 0, 0.0)
                        .otherwise((df["trip_miles"] / df["trip_time"] * 1609.344).cast("decimal(10,2)"))) \
            .withColumn("fare_per_min", when(df["trip_time"] == 0, 0.0)
                        .otherwise((df["totals_amount"] / df["trip_time"] * 60).cast("decimal(10,2)"))) \
            .withColumn("fare_per_mile", when(df["trip_miles"] == 0, 0.0)
                        .otherwise((df["totals_amount"] / df["trip_miles"]).cast("decimal(10,2)"))) \
            .withColumn("differ_pay_proportion",
                        ((df['base_passenger_fare'] - df['driver_pay']) / df['base_passenger_fare']).cast(
                            "decimal(10,2)")) \
            .withColumn("differ_surcharge_total",
                        (df["total_surcharge"] / df["totals_amount"]).cast("decimal(10,2)"))
        df = df.withColumn("pickup_time_id", date_format(df["pickup_datetime"], "HHmm"))
        df = df.withColumn("dropoff_time_id", date_format(df["dropoff_datetime"], "HHmm"))
        df = df.withColumn("flags_key",
                           concat(col("shared_request_flag"),
                                  col("shared_match_flag"),
                                  col("access_a_ride_flag"),
                                  col("wav_request_flag"),
                                  col("wav_match_flag"))) \
            .drop("shared_request_flag", "shared_match_flag", "access_a_ride_flag", "wav_request_flag",
                  "wav_match_flag")
        df = df.drop("_change_type", "_commit_version", "_commit_timestamp")
        df.printSchema()
        target_checkpoint_location = f"{self.fhvhv_trip_tbl}/_checkpoint"
        # enable CDF
        # self.spark.sql(f"ALTER TABLE delta.`{self.fhvhv_trip_tbl}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print("Starting write to silver")
        stream_query = df.writeStream \
            .format('delta') \
            .option("checkpointLocation", target_checkpoint_location) \
            .trigger(availableNow=True) \
            .start(self.fhvhv_trip_tbl)
        stream_query.awaitTermination()
        print("Done Streaming")

    def yellow_transform(self):
        time_tracking = self.spark.range(1) \
            .selectExpr("current_timestamp() - INTERVAL 2 HOURS as start_time") \
            .collect()[0]['start_time']
        df = self.spark.readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", time_tracking) \
            .option("ignoreDeletes", "true") \
            .load(f"s3a://{self.bucket_name}/bronze/yellow_tripdata")
        uuid_udf = udf(generate_uuid, StringType())
        df = df.withColumn("passenger_count", df["passenger_count"].cast(IntegerType())) \
            .withColumn("RatecodeID", df["RatecodeID"].cast(IntegerType())) \
            .withColumn("id", uuid_udf())

        df = df.withColumnRenamed('PULocationID', 'pickup_location_id') \
            .withColumn('trip_duration', (df["tpep_dropoff_datetime"].cast('bigint') / 1000) - (
                df["tpep_pickup_datetime"].cast('bigint') / 1000)) \
            .withColumnRenamed('DOLocationID', 'dropoff_location_id') \
            .withColumnRenamed('payment_type', 'payment_id') \
            .withColumnRenamed('VendorID', 'vendor_id') \
            .withColumnRenamed('RateCodeID', 'rate_code_id') \
            .withColumn("pickup_datetime",
                        from_unixtime((df["tpep_pickup_datetime"].cast('bigint') / 1000)).cast('timestamp')) \
            .withColumn("dropoff_datetime",
                        from_unixtime((df["tpep_dropoff_datetime"].cast('bigint') / 1000)).cast('timestamp'))
        df = df.withColumn("pickup_date_id", date_format(df["pickup_datetime"], 'yyyyMMdd')) \
            .withColumn("dropoff_date_id", date_format(df["dropoff_datetime"], 'yyyyMMdd')) \
            .withColumn("avg_speed", when(df["trip_duration"] == 0, 0.0)
                        .otherwise((df["trip_distance"] / df["trip_duration"] * 1609.344).cast("decimal(10,2)"))) \
            .withColumn("fare_per_min", when(df["trip_duration"] == 0, 0.0)
                        .otherwise((df["total_amount"] / df["trip_duration"] * 60).cast("decimal(10,2)"))) \
            .withColumn("fare_per_mile", when(df["trip_distance"] == 0, 0.0)
                        .otherwise((df["total_amount"] / df["trip_distance"]).cast("decimal(10,2)"))) \
            .withColumn("total_surcharge", (df['total_amount'] - df['fare_amount']).cast("decimal(10,2)")) \
            .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
        df = df.withColumn("differ_surcharge_total",
                           (df["total_surcharge"] / df["total_amount"]).cast("decimal(10,2)"))
        df = df.withColumn("pickup_time_id", date_format(df["pickup_datetime"], "HHmm")) \
            .withColumn("dropoff_time_id", date_format(df["dropoff_datetime"], "HHmm"))
        df = df.drop("_change_type", "_commit_version", "_commit_timestamp")

        target_checkpoint_location = f"{self.yellow_trip_tbl}/_checkpoint"
        # self.spark.sql(f"ALTER TABLE delta.`{self.yellow_trip_tbl}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print("Starting to write to silver")
        stream_query = df.writeStream \
            .trigger(availableNow=True) \
            .option("checkpointLocation", target_checkpoint_location) \
            .start()
        stream_query.awaitTermination()
        print("Done Streaming")

    def process_yellow_batch(self, batch_df, batch_id):
        print(f"Processing batch {batch_id}")
        batch_df.write \
            .format("console") \
            .mode("append")


    def process_fhvhv_batch(self, batch_df, batch_id):
        print(f"Processing batch {batch_id}")
        batch_df.write \
            .format("console") \
            .mode("append") \
            .save(self.fhvhv_trip_tbl)

    def test_streaming(self):
        target_location = f"{self.silver_location}/yellow_trip2"
        target_checkpoint_location = f"{target_location}/_checkpoint"
        df = self.spark.readStream.format("delta").load(f"s3a://{self.bucket_name}/bronze/yellow_tripdata")
        query = df.writeStream \
            .foreachBatch(self.process_batch) \
            .option("checkpointLocation", target_checkpoint_location) \
            .start()
        query.awaitTermination()
