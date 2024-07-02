import uuid
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging


class GoldDataProcessing(object):
    def __init__(self, bucket_name: str, spark: SparkSession):
        self.gold_location = f"s3a://{bucket_name}/gold"
        self.silver_location = f"s3a://{bucket_name}/silver"
        self.bronze_location = f"s3a://{bucket_name}/bronze"
        self.spark = spark

    # apply SCD2 for dim_location
    def update_gold_location(self):
        dim_location = DeltaTable.forPath(sparkSession=self.spark, path=f"{self.gold_location}/dim_location_t", )
        bronze_location_df = self.spark.read \
            .format("delta").load(f"{self.bronze_location}/location")
        new_location_to_insert = bronze_location_df \
            .alias("updates") \
            .join(dim_location.toDF().alias("location"), "location_id") \
            .where("location.is_active = true AND updates.service_zone <> location.service_zone")

        staged_updates = (
            new_location_to_insert
            .selectExpr("NULL as merge_key", "updates.*")
            .union(bronze_location_df.selectExpr("location_id as merge_key", "*"))
        )

        dim_location.alias("location").merge(
            staged_updates.alias("staged_updates"),
            "location.location_id = merge_key") \
            .whenMatchedUpdate(
            condition="location.is_active = true AND location.service_zone <> staged_updates.service_zone",
            set={
                "is_active": "false",
                "end_date": "staged_updates.effective_date"
            }
        ).whenNotMatchedInsert(
            values={
                "location_id": "staged_updates.location_id",
                "borough": "staged_updates.borough",
                "zone": "staged_updates.zone",
                "service_zone": "staged_updates.service_zone",
                "is_active": "true",
                "effective_date": "staged_updates.effective_date",
                "end_date": "null"
            }
        ).execute()

    def update_dpc_base_num(self):
        dim_dpc_base_num = DeltaTable.forPath(sparkSession=self.spark, path=f"{self.gold_location}/dim_dpc_base_num_t")
        bronze_dpc_base_num_df = self.spark.read \
            .format("delta") \
            .load(f"{self.bronze_location}/dpc_base_num")
        new_dpc_to_insert = bronze_dpc_base_num_df \
            .alias("updates") \
            .join(dim_dpc_base_num.toDF().alias("dpc_base_num"), "id") \
            .where("dpc_base_num.is_active = true AND updates.app_company <> dpc_base_num.app_company")
        bronze_dpc_base_num_df = bronze_dpc_base_num_df.select(
            *['id', 'base_num', "base_name", 'app_company', 'effective_date'])
        staged_updates = (
            new_dpc_to_insert
            .selectExpr("NULL as merge_key", "updates.*")
            .union(bronze_dpc_base_num_df.selectExpr("id as merge_key", "*"))
        )
        dim_dpc_base_num.alias("dpc_base_num").merge(
            staged_updates.alias("staged_updates"),
            "dpc_base_num.id = merge_key") \
            .whenMatchedUpdate(
            condition="dpc_base_num.is_active = true AND dpc_base_num.app_company <> staged_updates.app_company",
            set={
                "is_active": "false",
                "end_date": "staged_updates.effective_date"
            }
        ).whenNotMatchedInsert(
            values={
                "base_num": "staged_updates.base_num",
                "base_name": "staged_updates.base_name",
                "app_company": "staged_updates.app_company",
                "id": "staged_updates.id",
                "is_active": "true",
                "effective_date": "staged_updates.effective_date",
                "end_date": "null"
            }
        ).execute()

    def upsert_to_delta_tbl(self, batch_df, batch_id):
        fact_table = DeltaTable.forPath(self.spark, f"s3a://{self.gold_location}/fact_yellow_trip_t")
        fact_table.alias("core").merge(
            batch_df.alias("staging"),
            "core.id_pk == staging.id_pk"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def update_fact_yellow_trip(self):
        dim_location_path = f"{self.gold_location}/dim_location_t"
        dim_date_path = f"{self.gold_location}/dim_date_t"
        dim_payment_path = f"{self.gold_location}/dim_payment_t"
        dim_rate_code_path = f"{self.gold_location}/dim_rate_code_t"
        dim_time_path = f"{self.gold_location}/dim_time_t"
        silver_yellow_trip = f"{self.silver_location}/yellow_trip"

        dim_date_df = self.spark.read.format("delta").load(dim_date_path)
        dim_time_df = self.spark.read.format("delta").load(dim_time_path)
        dim_pickup_time = dim_time_df.withColumnRenamed("id", "pickup_time_id")
        dim_dropoff_time = dim_time_df.withColumnRenamed("id", "dropoff_time_id")
        dim_location_df = self.spark.read.format("delta").load(dim_location_path)
        dim_pickup_location = dim_location_df.withColumnRenamed("location_id", "pickup_location_id")
        dim_dropoff_location = dim_location_df.withColumnRenamed("location_id", "dropoff_location_id")
        dim_payment_df = self.spark.read.format("delta").load(dim_payment_path)
        dim_rate_code_df = self.spark.read.format("delta").load(dim_rate_code_path)
        start_time = self.spark.range(1) \
            .selectExpr("current_timestamp() - INTERVAL 2 HOURS as start_time") \
            .collect()[0]['start_time']
        fact_yellow_trip_df = self.spark \
            .readStream \
            .format("delta") \
            .option("ignoreChange", "true") \
            .option("startingTimestamp", start_time) \
            .option("readChangeFeed", "true") \
            .load(silver_yellow_trip)

        fact_yellow_trip_df = fact_yellow_trip_df \
            .join(dim_date_df, fact_yellow_trip_df["pickup_date_id"] == dim_date_df["date_id"], "left") \
            .join(dim_payment_df, fact_yellow_trip_df["payment_id"] == dim_payment_df["payment_id"], "left") \
            .join(dim_pickup_location.alias("b"),
                  "pickup_location_id",
                  "left").where('b.is_active = true') \
            .join(dim_dropoff_location.alias("c")
                  , "dropoff_location_id",
                  "left").where("c.is_active = true") \
            .join(dim_rate_code_df, fact_yellow_trip_df["rate_code_id"] == dim_rate_code_df["rate_code_id"], "left") \
            .join(dim_pickup_time, fact_yellow_trip_df["pickup_time_id"] == dim_pickup_time["pickup_time_id"], "left") \
            .join(dim_dropoff_time, fact_yellow_trip_df["dropoff_time_id"] == dim_dropoff_time["dropoff_time_id"],
                  "left") \
            .select(
            fact_yellow_trip_df["id"],
            fact_yellow_trip_df['id'].alias('id_pk'),
            dim_date_df['date_id'].alias('date_fk'),
            dim_pickup_time['pickup_time_id'].alias('pickup_time_fk'),
            dim_dropoff_time['dropoff_time_id'].alias('dropoff_time_fk'),
            dim_payment_df['payment_id'].alias('payment_fk'),
            dim_pickup_location['pickup_location_id'].alias('pickup_location_fk'),
            dim_dropoff_location['dropoff_location_id'].alias('dropoff_location_fk'),
            dim_rate_code_df['rate_code_id'].alias('rate_code_fk'),
            fact_yellow_trip_df["vendor_id"],
            fact_yellow_trip_df["passenger_count"],
            fact_yellow_trip_df['trip_distance'].alias('trip_miles'),
            fact_yellow_trip_df['trip_duration'].alias('trip_time'),
            fact_yellow_trip_df['fare_amount'],
            fact_yellow_trip_df['mta_tax'],
            fact_yellow_trip_df['tip_amount'],
            fact_yellow_trip_df['congestion_surcharge'],
            fact_yellow_trip_df['total_amount'],
            fact_yellow_trip_df['total_surcharge'],
            fact_yellow_trip_df['differ_surcharge_total'],
            fact_yellow_trip_df['avg_speed'],
            fact_yellow_trip_df['fare_per_min'],
            fact_yellow_trip_df['fare_per_mile'],
        )
        target_location = f"{self.gold_location}/fact_yellow_trip_t"

        stream_query = fact_yellow_trip_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .trigger(availableNow=True) \
            .option("checkpointLocation", f"{target_location}/_checkpoint") \
            .start(target_location)
        stream_query.awaitTermination()

    def update_fact_fhvhv_trip(self):
        dim_location_path = f"{self.gold_location}/dim_location_t"
        dim_date_path = f"{self.gold_location}/dim_date_t"
        dim_time_path = f"{self.gold_location}/dim_time_t"
        silver_fhvhv_trip_path = f"{self.silver_location}/fhvhv_trip"
        dim_hvfhs_license_num_path = f"{self.gold_location}/dim_hvfhs_license_num_t"
        dim_dpc_base_num_path = f"{self.gold_location}/dim_dpc_base_num_t"

        dim_hvfhs_license_num_df = self.spark.read.format("delta").load(dim_hvfhs_license_num_path)
        dim_dpc_base_num_df = self.spark.read.format("delta").load(dim_dpc_base_num_path)
        dim_date_df = self.spark.read.format("delta").load(dim_date_path)
        dim_time_df = self.spark.read.format("delta").load(dim_time_path)
        dim_pickup_time = dim_time_df.withColumnRenamed("id", "pickup_time_id")
        dim_dropoff_time = dim_time_df.withColumnRenamed("id", "dropoff_time_id")
        dim_location_df = self.spark.read.format("delta").load(dim_location_path).alias("location")
        dim_pickup_location = dim_location_df.withColumnRenamed("location_id", "pickup_location_id")
        dim_dropoff_location = dim_location_df.withColumnRenamed("location_id", "dropoff_location_id")
        start_time = self.spark.range(1) \
            .selectExpr("current_timestamp() - INTERVAL 2 HOURS as start_time") \
            .collect()[0]['start_time']
        fact_fhvhv_trip_df = self.spark \
            .readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", start_time) \
            .load(silver_fhvhv_trip_path)
        fact_fhvhv_trip_df = fact_fhvhv_trip_df.alias("fact") \
            .join(dim_date_df, fact_fhvhv_trip_df["pickup_date_id"] == dim_date_df['date_id'], "left") \
            .join(dim_pickup_time, fact_fhvhv_trip_df["pickup_time_id"] == dim_pickup_time["pickup_time_id"], "left") \
            .join(dim_dropoff_time, fact_fhvhv_trip_df["dropoff_time_id"] == dim_dropoff_time["dropoff_time_id"],
                  "left") \
            .join(dim_pickup_location.alias("b"),
                  "pickup_location_id",
                  "left").where('b.is_active = true') \
            .join(dim_dropoff_location.alias("c")
                  , "dropoff_location_id",
                  "left").where("c.is_active = true") \
            .join(dim_hvfhs_license_num_df,
                  fact_fhvhv_trip_df['license_num_id'] == dim_hvfhs_license_num_df['hvfhs_license_num'],
                  "left") \
            .join(dim_dpc_base_num_df.alias("f"),
                  fact_fhvhv_trip_df['base_num_id'] == dim_dpc_base_num_df['base_num'],
                  "left").where('f.is_active = true') \
            .select(
            fact_fhvhv_trip_df["id"],
            dim_date_df['date_id'].alias('date_fk'),
            dim_pickup_time['pickup_time_id'].alias('pickup_time_id_fk'),
            dim_dropoff_time['dropoff_time_id'].alias('dropoff_time_id_fk'),
            dim_pickup_location['pickup_location_id'].alias('pickup_location_id_fk'),
            dim_dropoff_location['dropoff_location_id'].alias('dropoff_location_id_fk'),
            dim_hvfhs_license_num_df["license_id"].alias('license_id_fk'),
            dim_dpc_base_num_df['base_num'].alias('base_id_fk'),
            fact_fhvhv_trip_df['trip_miles'],
            fact_fhvhv_trip_df['trip_time'],
            fact_fhvhv_trip_df['base_passenger_fare'],
            fact_fhvhv_trip_df['tolls'],
            fact_fhvhv_trip_df['sales_tax'],
            fact_fhvhv_trip_df['congestion_surcharge'],
            fact_fhvhv_trip_df['tips'],
            fact_fhvhv_trip_df['driver_pay'],
            fact_fhvhv_trip_df['totals_amount'],
            fact_fhvhv_trip_df['avg_speed'],
            fact_fhvhv_trip_df['fare_per_mile'],
            fact_fhvhv_trip_df['fare_per_min'],
            fact_fhvhv_trip_df["total_surcharge"],
            fact_fhvhv_trip_df["differ_pay_proportion"],
            fact_fhvhv_trip_df["differ_surcharge_total"]
        )
        target_location = f"{self.gold_location}/fact_fhvhv_trip_t"
        stream_query = fact_fhvhv_trip_df \
            .writeStream.format("delta") \
            .outputMode("append") \
            .trigger(availableNow=True) \
            .option("checkPointLocation", f"{target_location}/check_point") \
            .start(target_location)
        stream_query.awaitTermination()

    def update_fact_yellow_tracking_location_daily(self):
        dim_location_path = f"{self.gold_location}/dim_location_t"
        dim_date_path = f"{self.gold_location}/dim_date_t"
        silver_yellow_trip = f"{self.silver_location}/yellow_trip"
        dim_date_df = self.spark.read.format("delta").load(dim_date_path)
        dim_location_df = self.spark.read.format("delta").load(dim_location_path)
        dim_pickup_location = dim_location_df.withColumnRenamed("location_id", "pickup_location_id")
        dim_dropoff_location = dim_location_df.withColumnRenamed("location_id", "dropoff_location_id")

        start_time = self.spark.range(1) \
            .selectExpr("current_timestamp() - INTERVAL 5 HOURS as start_time") \
            .collect()[0]['start_time']
        yellow_trip_df = self.spark \
            .readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", start_time) \
            .load(silver_yellow_trip)

        fact_yellow_trip_df = yellow_trip_df.filter(yellow_trip_df["trip_distance"] != 0)

        dim_date_df_alias = dim_date_df.alias("d")
        dim_pickup_location_alias = dim_pickup_location.alias("pl")
        dim_dropoff_location_alias = dim_dropoff_location.alias("dl")
        fact_yellow_trip_df_alias = fact_yellow_trip_df.alias("fyt")

        result_df = fact_yellow_trip_df_alias \
            .join(dim_date_df_alias, fact_yellow_trip_df_alias["pickup_date_id"] == dim_date_df_alias['date_id'],
                  "left") \
            .join(dim_pickup_location_alias,
                  fact_yellow_trip_df_alias['pickup_location_id'] == dim_pickup_location_alias["pickup_location_id"],
                  "left") \
            .join(dim_dropoff_location_alias,
                  fact_yellow_trip_df_alias['dropoff_location_id'] == dim_dropoff_location_alias['dropoff_location_id'],
                  "left") \
            .groupby(
            fact_yellow_trip_df_alias["pickup_date_id"],
            fact_yellow_trip_df_alias["pickup_location_id"],
            fact_yellow_trip_df_alias["dropoff_location_id"]
        ) \
            .agg(
            count("*").alias("nums_trip"),
            sum("trip_distance").cast("decimal(10,2)").alias("totals_distance"),
            sum("total_amount").cast("decimal(10,2)").alias("totals_fare_amount"),
            avg("total_surcharge").cast("decimal(10,2)").alias("avg_total_surcharge"),
            avg("trip_duration").cast("decimal(10,2)").alias("avg_time_per_trip"),
            sum("congestion_surcharge").cast("decimal(10,2)").alias("total_congestion_surcharge"),
            avg("trip_distance").cast("decimal(10,2)").alias("avg_distance_per_trip"),
            avg("total_amount").cast("decimal(10,2)").alias("avg_total_amount_per_trip")
        ) \
            .withColumn("tracking_id", concat(
            col("pickup_date_id"),
            col("pickup_location_id"),
            col('dropoff_location_id'),
            unix_timestamp(current_timestamp()).cast("integer")
        )) \
            .select(
            col("tracking_id"),
            col("date_id").alias("date_id_fk"),
            col("pickup_location_id").alias("pickup_location_id_fk"),
            col("dropoff_location_id").alias("dropoff_location_id_fk"),
            col("nums_trip"),
            col("totals_distance"),
            col("totals_fare_amount"),
            col("avg_total_surcharge"),
            col("avg_time_per_trip"),
            col("total_congestion_surcharge"),
            col("avg_distance_per_trip"),
            col("avg_total_amount_per_trip")
        )
        target_location = f"{self.gold_location}/fact_yellow_tracking_location_daily_t"

        stream_query = result_df.writeStream \
            .format("delta") \
            .outputMode("complete") \
            .trigger(availableNow=True) \
            .option("checkpointLocation", f"{target_location}/_checkpoint") \
            .start(target_location)
        stream_query.awaitTermination()

    def update_fact_fhvhv_tracking_location_daily(self):
        dim_location_path = f"{self.gold_location}/dim_location_t"
        dim_date_path = f"{self.gold_location}/dim_date_t"
        silver_fhvhv_trip = f"{self.silver_location}/fhvhv_trip"
        dim_date_df = self.spark.read.format("delta").load(dim_date_path)
        dim_location_df = self.spark.read.format("delta").load(dim_location_path)
        dim_pickup_location = dim_location_df.withColumnRenamed("location_id", "pickup_location_id")
        dim_dropoff_location = dim_location_df.withColumnRenamed("location_id", "dropoff_location_id")

        start_time = self.spark.range(1) \
            .selectExpr("current_timestamp() - INTERVAL 5 HOURS as start_time") \
            .collect()[0]['start_time']
        fhvhv_trip_df = self.spark \
            .readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", start_time) \
            .load(silver_fhvhv_trip)

        fact_fhvhv_trip_df = fhvhv_trip_df.filter(fhvhv_trip_df["trip_miles"] != 0)

        dim_date_df_alias = dim_date_df.alias("date")
        dim_pickup_location_alias = dim_pickup_location.alias("pol")
        dim_dropoff_location_alias = dim_dropoff_location.alias("dol")
        fact_fhvhv_trip_df_alias = fact_fhvhv_trip_df.alias("ffhvt")

        result_df = fact_fhvhv_trip_df_alias \
            .join(dim_date_df_alias, fact_fhvhv_trip_df_alias["pickup_date_id"] == dim_date_df_alias['date_id'], "left") \
            .join(dim_pickup_location_alias,
                  fact_fhvhv_trip_df_alias['pickup_location_id'] == dim_pickup_location_alias["pickup_location_id"],
                  "left") \
            .join(dim_dropoff_location_alias,
                  fact_fhvhv_trip_df_alias['dropoff_location_id'] == dim_dropoff_location_alias['dropoff_location_id'],
                  "left") \
            .groupby(
            fact_fhvhv_trip_df_alias["pickup_date_id"],
            fact_fhvhv_trip_df_alias["pickup_location_id"],
            fact_fhvhv_trip_df_alias["dropoff_location_id"]
        ) \
            .agg(
            count("*").alias("nums_trip"),
            sum("trip_miles").cast("decimal(10,2)").alias("totals_distance"),
            sum("totals_amount").cast("decimal(10,2)").alias("totals_fare_amount"),
            avg("total_surcharge").cast("decimal(10,2)").alias("avg_total_surcharge"),
            avg("trip_time").cast("decimal(10,2)").alias("avg_time_per_trip"),
            sum("congestion_surcharge").cast("decimal(10,2)").alias("total_congestion_surcharge"),
            avg("trip_miles").cast("decimal(10,2)").alias("avg_miles_per_trip"),
            avg("totals_amount").cast("decimal(10,2)").alias("avg_total_amount_per_trip"),
            avg("driver_pay").cast("decimal(10,2)").alias("avg_driver_paid_per_trip")
        ) \
            .withColumn("tracking_id", concat(
        col("pickup_date_id"),
            col("pickup_location_id"),
            col("dropoff_location_id"),
            unix_timestamp(current_timestamp()).cast("integer")
        )) \
            .select(
            col("tracking_id"),
            col("pickup_date_id").alias("date_id_fk"),
            col("pickup_location_id").alias("pickup_location_id_fk"),
            col("dropoff_location_id").alias("dropoff_location_id_fk"),
            col("nums_trip"),
            col("totals_distance"),
            col("totals_fare_amount"),
            col("avg_total_surcharge"),
            col("avg_time_per_trip"),
            col("total_congestion_surcharge"),
            col("avg_miles_per_trip"),
            col("avg_total_amount_per_trip"),
            col("avg_driver_paid_per_trip")
        )

        
        result_df.printSchema()
        target_location = f"{self.gold_location}/fact_fhvhv_tracking_location_daily_t"

        stream_query = result_df.writeStream \
            .format("delta") \
            .outputMode("complete") \
            .trigger(availableNow=True) \
            .option("checkpointLocation", f"{target_location}/_checkpoint") \
            .start(target_location)
        stream_query.awaitTermination()
