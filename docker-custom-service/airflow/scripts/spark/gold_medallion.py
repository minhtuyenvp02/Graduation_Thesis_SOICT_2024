import uuid
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_executor import create_spark_session
import logging

class Gold(object):
    def __init__(self, bucket_name: str, spark: SparkSession):
        self.gold_location = f"s3a://{bucket_name}/gold"
        self.silver_location = f"s3a://{bucket_name}/silver"
        self.bronze_location = f"s3a://{bucket_name}/bronze"
        self.spark = spark

    # apply SCD2 for dim_location
    def update_gold_location(self):
        dim_location = DeltaTable.forPath(sparkSession=self.spark, path=f"{self.gold_location}/dim_location_t", )
        bronze_location_df = self.spark.read.format("delta").load(f"{self.bronze_location}/location")

        bronze_location_df = bronze_location_df.withColumn("is_active", lit(True))
        dim_active_location_df = dim_location.toDF().filter("is_active = True")
        join_condition = [dim_active_location_df["location_id"] == bronze_location_df["location_id"]]

        update_df = bronze_location_df.join(dim_active_location_df, join_condition, "left_anti")
        location_expire_df = bronze_location_df.join(dim_active_location_df, join_condition, "inner") \
            .select(dim_active_location_df["*"]) \
            .withColumn("is_active", lit(False))

        final_df = update_df.union(location_expire_df)

        dim_location.alias("dim").merge(
            final_df.alias("updates"),
            "dim.location_id = updates.location_id AND dim.is_active = True"
        ) \
            .whenMatchedUpdate(set={"is_active": expr("updates.is_active"), "location_id": expr("updates.location_id"),
                                    "borough": expr("updates.borough"), "zone": expr("updates.zone"),
                                    "service_zone": expr("updates.service_zone")}) \
            .whenNotMatchedInsertAll() \
            .execute()

    # apply SCD2 for dim_dpc_base_num
    def update_dpc_base_num(self):
        dim_dpc_base_num = DeltaTable.forPath(sparkSession=self.spark, path=f"{self.gold_location}/dim_dpc_base_num_t")
        bronze_dpc_base_num_df = self.spark.read.format("delta").load(f"{self.bronze_location}/dpc_base_num")

        bronze_dpc_base_num_df = bronze_dpc_base_num_df.withColumn("is_active", lit(True))
        active_dpc_base_num_df = dim_dpc_base_num.toDF().filter("is_active=True")
        join_condition = [active_dpc_base_num_df["id"] == bronze_dpc_base_num_df["id"]]

        update_df = bronze_dpc_base_num_df.join(active_dpc_base_num_df, join_condition, "left_anti")
        dpc_base_num_expire_df = bronze_dpc_base_num_df.join(active_dpc_base_num_df, join_condition, "inner") \
            .select(active_dpc_base_num_df["*"]) \
            .withColumn("is_active", lit(False))

        final_df = update_df.union(dpc_base_num_expire_df)

        dim_dpc_base_num.alias("dim").merge(
            final_df.alias("updates"),
            "dim.id = updates.id AND dim.is_active = True") \
            .whenMatchedUpdate(
            set={"is_active": expr("updates.is_active"), "hv_license_num": expr("updates.hv_license_num"),
                 "license_num": expr("updates.license_num"), "base_name": expr("updates.base_name"),
                 "app_company": expr("updates.app_company")}) \
            .whenNotMatchedInsertAll() \
            .execute()

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
            .join(dim_pickup_location,
                  fact_yellow_trip_df["pickup_location_id"] == dim_pickup_location["pickup_location_id"], "left") \
            .join(dim_dropoff_location,
                  fact_yellow_trip_df["dropoff_location_id"] == dim_dropoff_location["dropoff_location_id"], "left") \
            .join(dim_rate_code_df, fact_yellow_trip_df["rate_code_id"] == dim_rate_code_df["rate_code_id"], "left") \
            .join(dim_pickup_time, fact_yellow_trip_df["pickup_time_id"] == dim_pickup_time["pickup_time_id"], "left") \
            .join(dim_dropoff_time, fact_yellow_trip_df["dropoff_time_id"] == dim_dropoff_time["dropoff_time_id"],
                  "left") \
            .select(
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

        table = DeltaTable.forPath(path=target_location, sparkSession=self.spark)
        table.optimize().executeZOrderBy(["date_fk", "pickup_location_fk"])

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
        dim_location_df = self.spark.read.format("delta").load(dim_location_path)
        dim_pickup_location = dim_location_df.withColumnRenamed("location_id", "pickup_location_id")
        dim_dropoff_location = dim_location_df.withColumnRenamed("location_id", "dropoff_location_id")

        start_time = self.spark.range(1) \
            .selectExpr("current_timestamp() - INTERVAL 2 HOURS as start_time") \
            .collect()[0]['start_time']
        fact_fhvhv_trip_df = self.spark \
            .readStream \
            .format("delta") \
            .load(silver_fhvhv_trip_path)
            # .option("readChangeFeed", "true") \
            # .option("startingTimestamp", start_time) \
            # .load(silver_fhvhv_trip_path)

        fact_fhvhv_trip_df = fact_fhvhv_trip_df \
            .join(dim_date_df, fact_fhvhv_trip_df["pickup_date_id"] == dim_date_df['date_id'], "left") \
            .join(dim_pickup_time, fact_fhvhv_trip_df["pickup_time_id"] == dim_pickup_time["pickup_time_id"], "left") \
            .join(dim_dropoff_time, fact_fhvhv_trip_df["dropoff_time_id"] == dim_dropoff_time["dropoff_time_id"],
                  "left") \
            .join(dim_pickup_location,
                  (fact_fhvhv_trip_df['pickup_location_id'] == dim_pickup_location["pickup_location_id"]) &
                  (dim_pickup_location['is_active'] == True), "left") \
            .join(dim_dropoff_location,
                  fact_fhvhv_trip_df['dropoff_location_id'] == dim_dropoff_location["dropoff_location_id"], "left") \
            .join(dim_hvfhs_license_num_df,
                  fact_fhvhv_trip_df["license_num_id"] == dim_hvfhs_license_num_df["hvfhs_license_num"], "left") \
            .join(dim_dpc_base_num_df,
                  fact_fhvhv_trip_df["base_num_id"].cast(StringType()) == dim_dpc_base_num_df['license_num'].cast(
                      StringType()), "left") \
            .select(
            dim_date_df['date_id'].alias('date_fk'),
            dim_pickup_time['pickup_time_id'].alias('pickup_time_id_fk'),
            dim_dropoff_time['dropoff_time_id'].alias('dropoff_time_id_fk'),
            dim_pickup_location['pickup_location_id'].alias('pickup_location_id_fk'),
            dim_dropoff_location['dropoff_location_id'].alias('dropoff_location_id_fk'),
            dim_hvfhs_license_num_df["license_id"].alias('license_id_fk'),
            dim_dpc_base_num_df['license_num'].alias('base_id_fk'),
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

        stream_query = fact_fhvhv_trip_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .trigger(availableNow=True)\
            .start()
            # .option("checkpointLocation", f"{target_location}/_checkpoint") \
            # .start(target_location)
        stream_query.awaitTermination()

        table = DeltaTable.forPath(path=target_location, sparkSession=self.spark)
        table.optimize().executeZOrderBy(["date_fk", "pickup_location_id_fk"])

    def update_fact_yellow_tracking_location_daily(self):
        dim_location_path = f"{self.gold_location}/dim_location_t"
        dim_date_path = f"{self.gold_location}/dim_date_t"
        silver_yellow_trip = f"{self.silver_location}/yellow_trip"
        dim_date_df = self.spark.read.format("delta").load(dim_date_path)
        dim_location_df = self.spark.read.format("delta").load(dim_location_path)
        dim_pickup_location = dim_location_df.withColumnRenamed("location_id", "pickup_location_id")
        dim_dropoff_location = dim_location_df.withColumnRenamed("location_id", "dropoff_location_id")

        start_time = self.spark.range(1) \
            .selectExpr("current_timestamp() - INTERVAL 2 HOURS as start_time") \
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
            col("pickup_location_id"),
            col('dropoff_location_id'),
            col("pickup_date_id")
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
            col("avg_distance_per_trip"),
            col("avg_total_amount_per_trip")
        )
        target_location = f"{self.gold_location}/fact_yellow_tracking_location_daily_t"

        stream_query = result_df.writeStream \
            .format("console") \
            .outputMode("complete") \
            .trigger(availableNow=True) \
            .start()
            # .option("checkpointLocation", f"{target_location}/_checkpoint") \
            # .start(target_location)
        stream_query.awaitTermination()

        table = DeltaTable.forPath(path=target_location, sparkSession=self.spark)
        table.optimize().executeZOrderBy(["date_id_fk", "pickup_location_id_fk"])

    def update_fact_fhvhv_tracking_location_daily(self):
        dim_location_path = f"{self.gold_location}/dim_location_t"
        dim_date_path = f"{self.gold_location}/dim_date_t"
        silver_fhvhv_trip = f"{self.silver_location}/fhvhv_trip"
        dim_date_df = self.spark.read.format("delta").load(dim_date_path)
        dim_location_df = self.spark.read.format("delta").load(dim_location_path)
        dim_pickup_location = dim_location_df.withColumnRenamed("location_id", "pickup_location_id")
        dim_dropoff_location = dim_location_df.withColumnRenamed("location_id", "dropoff_location_id")

        start_time = self.spark.range(1) \
            .selectExpr("current_timestamp() - INTERVAL 2 HOURS as start_time") \
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
            col("pickup_location_id"),
            col('dropoff_location_id'),
            col("pickup_date_id")
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

        target_location = f"{self.gold_location}/fact_fhvhv_tracking_location_daily_t"
        print('Starting load fhvhv tracking daily .........')

        stream_query = result_df.writeStream \
            .format("delta") \
            .outputMode("complete") \
            .trigger(availableNow=True) \
            .option("checkpointLocation", f"{target_location}/_checkpoint") \
            .start(target_location)
        stream_query.awaitTermination()

        table = DeltaTable.forPath(path=target_location, sparkSession=self.spark)
        table.optimize().executeZOrderBy(["date_id_fk", "pickup_location_id_fk"])


spark = create_spark_session(app_name="Gold Update FHV Fact", spark_cluster='local[*]',
                             s3_endpoint='http://152.42.164.18:30090', s3_access_key="admin",
                             s3_secret_key='admin123')
gold = Gold(bucket_name='nyc-trip-bucket', spark=spark)

gold.update_fact_fhvhv_trip()