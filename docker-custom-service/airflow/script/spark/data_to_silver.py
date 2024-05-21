from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from delta import *
from config import S3_CONFIG, SPARK_CONFIG, SCHEMA_CONFIG
from pyspark.sql.types import *
from schema import CustomSchema
from delta import DeltaTable


class Silver(object):
    def __init__(self, bucket_name: str, schema):
        self.bucket_name = bucket_name
        self.schema = schema
        self.silver_location = f"s3a://{self.bucket_name}/silver"
        builder = SparkSession.builder.appName("Silver") \
            .config("master", "local[2]") \
            .config("spark.sql.shuffle.partitions", 4) \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.optimize.repartition.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
        self.spark = (configure_spark_with_delta_pip(builder, extra_packages=[
            'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1'])
                      .getOrCreate())
        # add confs
        sc = self.spark.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", S3_CONFIG["fs.s3a.access.key"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", S3_CONFIG["fs.s3a.secret.key"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", S3_CONFIG["fs.s3a.endpoint"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    # def make_location_table(self):

    def fhvhv_transform(self):
        df = self.spark.readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .load(f"s3a://{self.bucket_name}/bronze/fhvhv_tripdata")

        df = df.fillna("N", ["access_a_ride_flag"]) \
            .fillna("0.0", ["airport_fee"]) \
            .fillna("unknown", ["originating_base_num"])

        df = df.withColumnRenamed("POLocationID", "pickup_location_id") \
            .withColumnRenamed("DOLocationID", "dropoff_location_id") \
            .withColumn("pickup_datetime",
                        from_unixtime((df["pickup_datetime"].cast('bigint') / 1000)).cast('timestamp')) \
            .withColumn("dropoff_datetime",
                        from_unixtime((df["dropoff_datetime"].cast('bigint') / 1000)).cast('timestamp')) \
            .withColumn("totals_amount", (df["base_passenger_fare"] + df["tolls"] + df["bcf"] 
                        + df["sales_tax"] + df["congestion_surcharge"] + df["airport_fee"] + df["tips"]).cast("decimal(10,2)"))

        df = df.withColumn("pickup_date_id",
                           date_format(df["pickup_datetime"], 'yyyyMMdd')) \
            .withColumn("dropoff_date_id",
                        date_format(df["dropoff_datetime"], 'yyyyMMdd')) \
            .withColumn("avg_speed", when(df["trip_time"] == 0, 0.0)
                        .otherwise((df["trip_miles"] / df["trip_time"] * 1609.344).cast("decimal(10,2)")))\
            .withColumn("fare_per_min", when(df["trip_time"] == 0, 0.0)
                        .otherwise((df["totals_amount"] / df["trip_time"] * 60).cast("decimal(10,2)")))\
            .withColumn("fare_per_mile", when(df["trip_miles"] == 0, 0.0)
                        .otherwise((df["totals_amount"] / df["trip_miles"]).cast("decimal(10,2)")))
        df = df.withColumn("pickup_time_24", date_format(df["pickup_datetime"], "HH:mm"))
        df = df.withColumn("dropoff_time_24", date_format(df["dropoff_datetime"], "HH:mm"))
        df.printSchema()
        target_location = f"{self.silver_location}/fhvhv_tripdata"
        target_checkpoint_location = f"{target_location}/_checkpoint"
        stream_query = df.writeStream \
            .format("console") \
            .trigger(availableNow=True) \
            .option("checkpoint_location", target_checkpoint_location) \
            .start(target_location)
        stream_query.awaitTermination()

    def yellow_transform(self):
        df = self.spark.readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .load(f"s3a://{self.bucket_name}/bronze/yellow_tripdata")
        df = df.withColumn("passenger_count", df["passenger_count"].cast(IntegerType()))\
                .withColumn("RatecodeID", df["RatecodeID"].cast(IntegerType()))

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
                        .otherwise((df["total_amount"] / df["trip_duration"] * 60).cast("decimal(10,2)")))\
            .withColumn("fare_per_mile", when(df["trip_distance"] == 0, 0.0)
                        .otherwise((df["total_amount"] / df["trip_distance"]).cast("decimal(10,2)")))\
            .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
        df = df.withColumn("pickup_time_24", date_format(df["pickup_datetime"], "HH:mm"))
        df = df.withColumn("dropoff_time_24", date_format(df["dropoff_datetime"], "HH:mm"))

        target_location = f"{self.silver_location}/yellow_trip"
        target_checkpoint_location = f"{target_location}/_checkpoint"
        stream_query = df.writeStream \
            .format("console") \
            .outputMode("append") \
            .trigger(availableNow=True)  \
            .option("checkpointLocation", target_checkpoint_location) \
            .start(target_location)
        stream_query.awaitTermination()


silver = Silver(bucket_name="nyc-trip-bucket", schema=CustomSchema(SCHEMA_CONFIG))
silver.yellow_transform()
silver.fhvhv_transform()

