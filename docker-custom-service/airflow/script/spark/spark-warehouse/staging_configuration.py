from delta import DeltaTable
from pyspark.sql.functions import *
import uuid


class StagingTableBuilder(object):
    def __init__(self, staging_location: str):
        self.staging_location = staging_location  # s3a://{bucket_name}/silver
        builder = SparkSession.builder.appName("MyApp") \
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

    def create_yellowtrip_tbl(self):
        self.spark.sql('CREATE DATABASE IF NOT EXISTS silver;')
        DeltaTable.createIfNotExists(self.spark) \
            .tableName("silver.yellow_trip") \
            .addColumn("trip_id", "STRING", nullable=False, generatedAlwaysAs="uuid.uuid4()") \
            .addColumn("vendor_id", "INT", nullable=False) \
            .addColumn("pickup_datetime", "TIMESTAMP", nullable=True) \
            .addColumn("dropoff_datetime", "TIMESTAMP", nullable=True) \
            .addColumn("pickup_date_id", "INT", nullable=False,
                       generatedAlwaysAs="DATE_FORMAT(pickup_datetime, \"yyyymmdd\")") \
            .addColumn("dropoff_date_id", "INT", nullable=False,
                       generatedAlwaysAs="DATE_FORMAT(dropoff_datetime, \"yyyymmdd\")") \
            .addColumn("passenger_count", "INT", nullable=False) \
            .addColumn("trip_distance", "DOUBLE", nullable=True) \
            .addColumn("rate_code_id", "SMALLINT", nullable=True) \
            .addColumn("store_and_fwd_flag", "BOLEAN", nullable=True) \
            .addColumn("pickup_location_id", "INT", nullable=False) \
            .addColumn("dropoff_location_id", "INT", nullable=False) \
            .addColumn("payment_id", "INT", nullable=True) \
            .addColumn("fare_amount", "DOUBLE", nullable=True) \
            .addColumn("extra", "DOUBLE", nullable=True) \
            .addColumn("mta_tax", "DOUBLE", nullable=True) \
            .addColumn("tip_amount", "DOUBLE", nullable=True) \
            .addColumn("tools_amount", "DOUBLE", nullable=True) \
            .addColumn("improvement_surchange", "DOUBLE", nullable=True) \
            .addColumn("trip_duration", "DOUBLE", nullable=True) \
            .addColumn("avg_speed", "DOUBLE", nullable=True) \
            .partitionedBy("trip_id", "pickup_datetime") \
            .location(f"{self.staging_location}/yellow_trip") \
            .execute()

    def create_fhvhv_trip(self):
        self.spark.sql('CREATE DATABASE IF NOT EXISTS silver;')
        DeltaTable.createIfNotExists(sparkSession=self.spark) \
            .tableName("fhvhv_trip") \
            .addColumn("trip_id", "INT", nullable=False, generatedAlwaysAs="uuid.uuid4()") \
            .addColumn("hvfhs_license_num", "STRING", nullable=False) \
            .addColumn("dispatching_base_num", "STRING", nullable=False) \
            .addColumn("originating_base_num", "STRING", nullable=True) \
            .addColumn("pickup_datetime", "TIMESTAMP", nullable=True) \
            .addColumn("dropoff_datetime", "TIMESTAMP", nullable=True) \
            .addColumn("pickup_date_id", "INT", nullable=False,
                       generatedAlwaysAs="DATE_FORMAT(pickup_datetime, \"yyyymmdd\")") \
            .addColumn("dropoff_date_id", "INT", nullable=False,
                       generatedAlwaysAs="DATE_FORMAT(dropoff_datetime, \"yyyymmdd\")") \
            .addColumn("request_datetime", "TIMESTAMP", nullable=True) \
            .addColumn("on_scene_datetime", "TIMESTAMP", nullable=True) \
            .addColumn("pickup_location_id", "INT", nullable=False) \
            .addColumn("dropoff_location_id", "INT", nullable=False) \
            .addColumn("trip_miles", "DOUBLE", nullable=True) \
            .addColumn("trip_time", "DOUBLE", nullable=False) \
            .addColumn("base_passenger_fare", "DOUBLE", nullable=False) \
            .addColumn("tolls", "DOUBLE", nullable=False) \
            .addColumn("bcf", "DOUBLE", nullable=False) \
            .addColumn("sales_tax", "DOUBLE", nullable=False) \
            .addColumn("congestion_surcharge", "DOUBLE", nullable=False) \
            .addColumn("airport_fee", "DOUBLE", nullable=False) \
            .addColumn("tips", "DOUBLE", nullable=False) \
            .addColumn("driver_pay", "DOUBLE", nullable=False) \
            .addColumn("shared_request_flag", "STRING", nullable=False) \
            .addColumn("shared_match_flag", "STRING", nullable=False) \
            .addColumn("access_a_ride_flag", "STRING", nullable=False) \
            .addColumn("wav_request_flag", "STRING", nullable=False) \
            .addColumn("wav_match_flag", "STRING", nullable=False) \
            .partitionedBy("trip_id", "pickup_datetime") \
            .location(f"{self.staging_location}/fhvhv_trip")

    def create_location_tbl(self):
        self.spark.sql('CREATE DATABASE IF NOT EXISTS silver;')
        DeltaTable.createIfNotExists(sparkSession=self.spark) \
            .tableName("location") \
            .addColumn("location_id", "INT", nullable=False) \
            .addColumn("borough", "STRING", nullable=False) \
            .addColumn("zone", "STRING", nullable=False) \
            .addColumn("service_zone", "STRING", nullable=False) \
            .location(f"{self.staging_location}/location") \
            .execute()

    def create_fhvhv_base_num_tbl(self):
        self.spark.sql('CREATE DATABASE IF NOT EXISTS silver;')
        DeltaTable.createIfNotExists(sparkSession=self.spark) \
            .tableName("fhvhv_basenum") \
            .addColumn("hv_license_num", "STRING", nullable=False) \
            .addColumn("license_num", "STRING", nullable=False) \
            .addColumn("base_name", "STRING", nullable=False) \
            .addColumn("app_company", "STRING", nullable=False) \
            .location(f"{self.staging_location}/hvfhv_dpc_base_num") \
            .execute()
