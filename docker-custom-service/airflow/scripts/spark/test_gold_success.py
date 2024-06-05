from spark_executor import create_spark_session

spark = create_spark_session(app_name="Test")
df = (spark.read.format("delta")
      .option("inferSchema", "true")
      .option("readChangeFeed", "true")
      .option("startingTimestamp", "2024-05-27 15:00:00")
      .load("s3a://nyc-trip-bucket/gold/fact_fhvhv_trip_t"))
df.show(100)