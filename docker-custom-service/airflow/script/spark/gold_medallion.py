import uuid

from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from config import *
from delta import configure_spark_with_delta_pip
from spark_executor import create_spark_session

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
        print("Starting merge........")
        dim_location.alias("dim").merge(
            final_df.alias("updates"),
            "dim.location_id = updates.location_id AND dim.is_active = True"
        ) \
            .whenMatchedUpdate(set={"is_active": expr("updates.is_active"), "location_id": expr("updates.location_id"),
                                    "borough": expr("updates.borough"), "zone": expr("updates.zone"),
                                    "service_zone": expr("updates.service_zone")}) \
            .whenNotMatchedInsertAll() \
            .execute()
        print("DONE......")

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
            .whenMatchedUpdate(set={"is_active": expr("updates.is_active"), "hv_license_num": expr("updates.hv_license_num")
                                    , "license_num": expr("updates.license_num"), "base_name": expr("updates.base_name")
                                    , "app_company": expr("updates.app_company")}) \
            .whenNotMatchedInsertAll() \
            .execute()

    def upsert_to_delta_tbl(self, batch_df, batch_id):
        batch_df.show(3)
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
        print("Loading df.......")
        dim_date_df = self.spark.read.format("delta").load(dim_date_path)
        print("dim_date loaded")
        dim_date_df.show(3)
        dim_time_df = self.spark.read.format("delta").load(dim_time_path)
        print("dim_time loaded")
        dim_time_df.show(3)
        dim_pickup_time = dim_time_df.withColumnRenamed("id", "pickup_time_id")
        print("dim_pickup_time")
        dim_pickup_time.show(3)
        dim_dropoff_time = dim_time_df.withColumnRenamed("id", "dropoff_time_id")
        dim_location_df = self.spark.read.format("delta").load(dim_location_path)
        print("dim_location loaded")
        dim_location_df.show(3)
        dim_pickup_location = dim_location_df.withColumnRenamed("location_id", "pickup_location_id")
        dim_dropoff_location = dim_location_df.withColumnRenamed("location_id", "dropoff_location_id")
        dim_payment_df = self.spark.read.format("delta").load(dim_payment_path)
        print("dim_payment loaded")
        dim_payment_df.show(3)
        dim_rate_code_df = self.spark.read.format("delta").load(dim_rate_code_path)
        print("dim_rate_code loaded")
        dim_rate_code_df.show(3)
        fact_yellow_trip_df = self.spark \
            .readStream \
            .format("delta") \
            .option("ignoreChange", "true") \
            .option("readChangeFeed", "true") \
            .load(silver_yellow_trip)
        print("yellow_trip_loaded")
        # fact_yellow_trip_df.show(3)
        print("Building fact........")
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
        print("Build Done")
        # fact_yellow_trip_df.show(3)
        print("Starting load to gold ......")
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
        print("Load done")
    def update_fact_fhvhv_trip(self):
        dim_location_path = f"{self.gold_location}/dim_location_t"
        dim_date_path = f"{self.gold_location}/dim_date_t"
        dim_time_path = f"{self.gold_location}/dim_time_t"
        silver_fhvhv_trip_path = f"{self.silver_location}/yellow_trip/fhvhv_trip"
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
            .selectExpr("current_timestamp() - INTERVAL 1 DAY as start_time") \
            .collect()[0]['start_time']
        fact_fhvhv_trip_df = self.spark \
            .read \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", start_time) \
            .load(silver_fhvhv_trip_path)
        fact_fhvhv_trip_df = fact_fhvhv_trip_df \
            .join(dim_date_df, fact_fhvhv_trip_df["pickup_date_id"] == dim_time_df['date_id'], "left") \
            .join(dim_pickup_time, fact_fhvhv_trip_df["pickup_time_id"] == dim_pickup_time["pickup_time_id"], "left") \
            .join(dim_dropoff_time, fact_fhvhv_trip_df["dropoff_time_id"] == dim_dropoff_time["dropoff_time_id"],
                  "left") \
            .join(dim_pickup_location,
                  fact_fhvhv_trip_df['pickup_location_id'] == dim_pickup_location["pickup_location_id"], "left") \
            .join(dim_dropoff_location,
                  fact_fhvhv_trip_df['dropoff_location_id'] == dim_dropoff_location["dropoff_location_id"], "left") \
            .join(dim_hvfhs_license_num_df,
                  fact_fhvhv_trip_df["license_num_id"] == dim_hvfhs_license_num_df["license_id"], "left") \
            .join(dim_dpc_base_num_df, fact_fhvhv_trip_df["base_num_id"] == dim_dpc_base_num_df['license_num'], "left") \
            .select(
            dim_date_df['date_id'].alias('date_fk'),
            dim_pickup_time['pickup_time_id'].alias('pickup_time_fk'),
            dim_dropoff_time['dropoff_time_id'].alias('dropoff_time_fk'),
            dim_pickup_location['pickup_location_id'].alias('pickup_location_fk'),
            dim_dropoff_location['dropoff_location_id'].alias('dropoff_location_fk'),
            dim_hvfhs_license_num_df["license_num_id"].alias('license_num_fk'),
            dim_dpc_base_num_df['base_num_id'].alias('base_num_fk'),
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
        print("Starting to write to fact_fhvhv_trip")
        stream_query = fact_yellow_trip_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .trigger(availableNow=True) \
            .option("checkpointLocation", f"{target_location}/_checkpoint") \
            .start(target_location)
        stream_query.awaitTermination()
        table = DeltaTable.forPath(path=target_location, sparkSession=self.spark)
        table.optimize().executeZOrderBy(["date_fk", "pickup_location_fk"])
        print("Done")

    def update_fact_yellow_tracking_location_daily(self):
        dim_location_path = f"{self.gold_location}/dim_location_t"
        dim_date_path = f"{self.gold_location}/dim_date_t"
        silver_yellow_trip = f"{self.silver_location}/yellow_trip"
        dim_date_df = self.spark.read.format("delta").load(dim_date_path)
        dim_location_df = self.spark.read.format("delta").load(dim_location_path)
        dim_pickup_location = dim_location_df.withColumnRenamed("location_id", "pickup_location_id")
        dim_dropoff_location = dim_location_df.withColumnRenamed("location_id", "dropoff_location_id")

        start_time = self.spark.range(1) \
            .selectExpr("current_timestamp() - INTERVAL 1 DAY as start_time") \
            .collect()[0]['start_time']
        yellow_trip_df = self.spark \
            .readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", start_time) \
            .load(silver_yellow_trip)

        fact_yellow_trip_df = yellow_trip_df.filter(yellow_trip_df["trip_distance"] != 0)
        fact_yellow_trip_df = fact_yellow_trip_df \
            .join(dim_date_df, fact_yellow_trip_df["pickup_date_id"] == dim_date_df['date_id'], "left") \
            .join(dim_pickup_location,
                  fact_yellow_trip_df['pickup_location_id'] == dim_pickup_location["pickup_location_id"], "left") \
            .join(dim_dropoff_location,
                  fact_yellow_trip_df['dropoff_location_id'] == dim_dropoff_location['dropoff_location_id']) \
            .groupby("pickup_date_id", "pickup_location_id", "dropoff_location_id") \
            .agg(
            count("*").alias("nums_trip"),
            sum("trip_distance").alias("totals_distance"),
            sum("total_amount").alias("totals_fare_amount"),
            avg("total_surcharge").alias("avg_total_surcharge"),
            avg("trip_duration").alias("avg_time_per_trip"),
            sum("congestion_surcharge").alias("total_congestion_surcharge"),
            avg("trip_distance").alias("avg_distance_per_trip"),
            avg("total_amount").alias("avg_total_amount_per_trip")
        ).withColumn("tracking_id", concat(col("pickup_location_id"),
                                           col('dropoff_location_id'),
                                           col("pickup_date_id"))) \
        .select("tracking_id",
                dim_date_df.date_id.alias("date_id"),
                dim_pickup_location.pickup_location_id,
                dim_dropoff_location.dropoff_location_id,
                "nums_trip", "totals_distance", "totals_fare_amount",
                "avg_total_surcharge", "avg_time_per_trip",
                "total_congestion_surcharge", "avg_miles_per_trip",
                "avg_total_amount_per_trip", )

        target_location = f"{self.gold_location}/fact_yellow_tracking_location_daily_t"
        table = DeltaTable.forPath(path=target_location, sparkSession=self.spark)
        table.optimize().executeZOrderBy(["date_id", "pickup_location_id"])
        fact_yellow_trip_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .trigger(availableNow=True) \
            .option("checkpointLocation", f"{target_location}/_checkpoint") \
            .start(target_location)

    def update_fact_fhvhv_tracking_location_daily(self):
        dim_location_path = f"{self.gold_location}/dim_location_t"
        dim_date_path = f"{self.gold_location}/dim_date_t"
        silver_fhvhv_trip = f"{self.silver_location}/fhvhv_trip"
        dim_date_df = self.spark.read.format("delta").load(dim_date_path)
        dim_location_df = self.spark.read.format("delta").load(dim_location_path)
        dim_pickup_location = dim_location_df.withColumnRenamed("location_id", "pickup_location_id")
        dim_dropoff_location = dim_location_df.withColumnRenamed("location_id", "dropoff_location_id")

        start_time = self.spark.range(1) \
            .selectExpr("current_timestamp() - INTERVAL 1 DAY as start_time") \
            .collect()[0]['start_time']
        fhvhv_trip_df = self.spark \
            .readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", start_time) \
            .load(silver_fhvhv_trip)

        fact_fhvhv_trip_df = fhvhv_trip_df.filter(fhvhv_trip_df["trip_distance"] != 0)
        fact_fhvhv_trip_df = fact_fhvhv_trip_df \
            .join(dim_date_df, fact_fhvhv_trip_df["pickup_date_id"] == dim_date_df['date_id'], "left") \
            .join(dim_pickup_location,
                  fact_fhvhv_trip_df['pickup_location_id'] == dim_pickup_location["pickup_location_id"], "left") \
            .join(dim_dropoff_location,
                  fact_fhvhv_trip_df['dropoff_location_id'] == dim_dropoff_location['dropoff_location_id']) \
            .groupby("pickup_date_id", "pickup_location_id", "dropoff_location_id") \
            .agg(
            count("*").alias("nums_trip"),
            sum("trip_miles").alias("totals_distance"),
            sum("totals_amount").alias("totals_fare_amount"),
            avg("total_surcharge").alias("avg_total_surcharge"),
            avg("trip_duration").alias("avg_time_per_trip"),
            sum("congestion_surcharge").alias("total_congestion_surcharge"),
            avg("trip_miles").alias("avg_miles_per_trip"),
            avg("totals_amount").alias("avg_total_amount_per_trip"),
            avg("driver_pay").alias("avg_driver_paid_per_trip")
        ).withColumn("tracking_id", concat(col("pickup_location_id"),
                                           col('dropoff_location_id'),
                                           col("pickup_date_id"))) \
            .select("tracking_id",
                    dim_date_df.date_id.alias("date_id"),
                    dim_pickup_location.pickup_location_id,
                    dim_dropoff_location.dropoff_location_id,
                    "nums_trip", "totals_distance", "totals_fare_amount",
                    "avg_total_surcharge", "avg_time_per_trip",
                    "total_congestion_surcharge", "avg_miles_per_trip",
                    "avg_total_amount_per_trip", "driver_paid_per_trip", )

        target_location = f"{self.gold_location}/fact_yellow_tracking_location_daily_t"
        table = DeltaTable.forPath(path=target_location, sparkSession=self.spark)
        table.optimize().executeZOrderBy(["date_id", "pickup_location_id"])
        fact_yellow_trip_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .trigger(availableNow=True) \
            .option("checkpointLocation", f"{target_location}/_checkpoint") \
            .start(target_location)


spark = create_spark_session(app_name="UpdateSCD2")
gold = Gold(bucket_name="nyc-trip-bucket", spark=create_spark_session("test_gold"))
gold.update_fact_yellow_trip()
