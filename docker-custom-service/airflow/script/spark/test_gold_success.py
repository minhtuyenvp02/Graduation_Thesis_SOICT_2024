from spark_executor import create_spark_session

spark = create_spark_session(app_name="Test")
df = (spark.read.format("delta")
      .option("inferSchema", "true")
      .load("s3a://nyc-trip-bucket/gold/fact_fhvhv_trip_t"))
print(df.count())
df.show(1000)