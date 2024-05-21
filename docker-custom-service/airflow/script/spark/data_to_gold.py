from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


class Gold(object):
    def __init__(self, bucket_name: str):
        self.gold_location = f"s3a://{bucket_name}/gold"
        self.silver_location = f"s3a://{bucket_name}/silver"
        self.bronze_location = f"s3a://{bucket_name}/bronze"
        builder = SparkSession.builder.appName("Gold") \
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

    # apply SCD2 for dim_location
    def update_gold_location(self):
        dim_location = DeltaTable.forPath(f"s3a://{self.gold_location}/dim_location_t")
        bronze_location_df = self.spark.read.format("delta").load(f"{self.bronze_location}/location")

        bronze_location_df = bronze_location_df.withColumn("is_active", lit(True))
        dim_active_location_df = dim_location.toDF().filter("is_active = True")
        join_condition = [dim_active_location_df["location_id"] == bronze_location_df["location_id"]]
        update_df = bronze_location_df.join(df_active_location, join_condition, "left-anti")
        location_expire_df = bronze_location_df.join(dim_active_location_df, join_condition, "inner") \
            .select(dim_active_location_df["*"]) \
            .withColumn("is_active", lit(False))
        final_df = update_df.union(location_expire_df)

        dim_location.alias("dim").merge(
            final_df.alias("updates"),
            "dim.location_id = updates.location_id AND dim.is_active = True"
        ) \
            .whenMatchedUpdate(set={"is_active": expr("updates.is_current")}) \
            .whenNotMatchedInsertAll() \
            .execute()

    # apply SCD2 for dim_dpc_base_num
    def update_dpc_base_num(self):
        dim_dpc_base_num = DeltaTable.forPath(f"s3a://{self.gold_location}/dim_dpc_base_num_t")
        bronze_dpc_base_num_df = self.spark.read.format("delta").load(f"{self.bronze_location}/fhvhv_dpc_base_num")

        bronze_dpc_base_num_df.withColumn("is_active", lit(True))
        active_dpc_base_num_df = dim_dpc_base_num.toDF().filter("is_active=True")
        join_condition = [active_dpc_base_num_df["location_id"] == bronze_dpc_base_num_df["location_id"]]
        update_df = bronze_dpc_base_num_df.join(active_dpc_base_num_df, join_condition, "left-anti")
        dpc_base_num_expire_df = bronze_dpc_base_num_df.join(active_dpc_base_num_df, join_condition, "inner") \
            .select(active_dpc_base_num_df["*"]) \
            .withColumn("is_active", lit(False))
        final_df = update_df.union(dpc_base_num_expire_df)
        dim_dpc_base_num.alias("dim").merge(
            final_df.alias("updates"),
            "dim.location_id = updates.location_id AND dim.is_active = True"
        ) \
            .whenMatchedUpdate(set={"is_active": expr("updates.is_current")}) \
            .whenNotMatchedInsertAll() \
            .execute()

    def update_fact_yellow_trip(self):
        


