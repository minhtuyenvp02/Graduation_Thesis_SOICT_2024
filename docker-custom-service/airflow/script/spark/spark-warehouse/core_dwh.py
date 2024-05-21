from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *
from delta import *


class WareHouseBuilder(object):
    def __init__(self, dwh_location: str, silver_location: str):
        self.silver_location = silver_location
        self.dwh_location = dwh_location
        builder = SparkSession.builder.appName("DWHBuilder") \
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

    def create_dim_date(self):
        # self.spark.sql('CREATE DATABASE IF NOT EXISTS gold;')
        # Define start and end dates
        start_date = "2020-01-01"
        end_date = "2025-12-31"

        # Create a DataFrame with a range of dates
        dates = self.spark.sql(f"""
            SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS calendar_date
        """)

        # Create the DataFrame with required columns
        dim_calendar = dates.select(
            (year(col("calendar_date")) * 10000 + month(col("calendar_date")) * 100 + dayofmonth(
                col("calendar_date"))).alias("date_int"),
            col("calendar_date"),
            year(col("calendar_date")).alias("calendar_year"),
            date_format(col("calendar_date"), 'MMMM').alias("calendar_month"),
            month(col("calendar_date")).alias("month_of_year"),
            date_format(col("calendar_date"), 'EEEE').alias("calendar_day"),
            dayofweek(col("calendar_date")).alias("day_of_week"),
            (expr("weekday(calendar_date)") + 1).alias("day_of_week_start_monday"),
            expr("CASE WHEN weekday(calendar_date) < 5 THEN 'Y' ELSE 'N' END").alias("is_week_day"),
            dayofmonth(col("calendar_date")).alias("day_of_month"),
            expr("CASE WHEN calendar_date = last_day(calendar_date) THEN 'Y' ELSE 'N' END").alias(
                "is_last_day_of_month"),
            dayofyear(col("calendar_date")).alias("day_of_year"),
            weekofyear(col("calendar_date")).alias("week_of_year_iso"),
            quarter(col("calendar_date")).alias("quarter_of_year"),
            expr(
                "CASE WHEN month(calendar_date) >= 10 THEN year(calendar_date) + 1 ELSE year(calendar_date) END").alias(
                "fiscal_year_oct_to_sep"),
            expr("(month(calendar_date) + 2) % 12 + 1").alias("fiscal_month_oct_to_sep"),
            expr("CASE WHEN month(calendar_date) >= 7 THEN year(calendar_date) + 1 ELSE year(calendar_date) END").alias(
                "fiscal_year_jul_to_jun"),
            expr("(month(calendar_date) + 5) % 12 + 1").alias("fiscal_month_jul_to_jun")
        ).orderBy(col("calendar_date"))

        # Define the path for Delta table
        delta_table_path = f"s3a://{self.dwh_location}/dim_date_t"

        # Write DataFrame to Delta table
        dim_calendar.write \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .option("delta.targetFileSize", "67108864") \
            .option("pipelines.autoOptimize.zOrderCols", "calendar_date") \
            .mode("overwrite") \
            .save(delta_table_path)
        self.spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS gold.dim_payment_t
                        USING DELTA
                        LOCATION '{dim_payment_path}'
                    """)

        # Set Delta table properties using SQL
        self.spark.sql(f"""
            ALTER TABLE delta.`{delta_table_path}`
            SET TBLPROPERTIES (
              'quality' = 'gold',
              'delta.targetFileSize' = '67108864',
              'pipelines.autoOptimize.zOrderCols' = 'calendar_date'
            )
        """)

    def create_dim_location(self):
        self.spark.sql('CREATE DATABASE IF NOT EXISTS gold;')
        DeltaTable.createIfNotExists(sparkSession=self.spark) \
            .tableName("gold.dim_location") \
            .addColumn("location_id", "INT", nullable=False) \
            .addColumn("borough", "STRING", nullable=True) \
            .addColumn("zone", "STRING", nullable=True) \
            .addColumn("service_zone", "STRING", nullable=True) \
            .addColumn("is_active", "BOOLEAN", nullable=True) \
            .location(f"s3a://{self.dwh_location}/dim_location") \
            .partitionedBy("location_id", "zone") \
            .comment("dwh.dim_location_t") \
            .execute()
        return

    def create_dim_dispatching_base_num(self):
        self.spark.sql('CREATE DATABASE IF NOT EXISTS gold;')
        DeltaTable.createIfNotExists(self.spark) \
            .tableName("gold.dim_dpc_base_num_t") \
            .addColumn("hv_license_num", "STRING", nullable=True) \
            .addColumn("license_num", "STRING", nullable=True) \
            .addColumn("base_name", "STRING", nullable=True) \
            .addColumn("app_company", "STRING", nullable=True) \
            .addColumn("is_active", "BOOLEAN", nullable=True) \
            .partitionedBy("license_num") \
            .comment("dwh.dim_dpc_base_num_t") \
            .execute()
        return

    def create_dim_payment(self):
        self.spark.sql('CREATE DATABASE IF NOT EXISTS gold;')
        schema = StructType([
            StructField("payment_id", IntegerType(), False),
            StructField("payment_type", StringType(), False)
        ])
        data = [
            (1, "Credit card"),
            (2, "Cash"),
            (3, "No charge"),
            (4, "Dispute"),
            (5, "Unknown"),
            (6, "Voided trip")
        ]

        df_dim_payment = self.spark.createDataFrame(data, schema)
        dim_payment_path = f"s3a://{self.dwh_location}/dim_payment_t"

        (df_dim_payment.write.format("delta")
         .option("overwriteSchema", "true")
         .mode("overwrite").save(dim_payment_path))
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS gold.dim_payment_t
            USING DELTA
            LOCATION '{dim_payment_path}'
        """)

        return

    def create_dim_flag(self):
        self.spark.sql('CREATE DATABASE IF NOT EXISTS gold;')
        poss_values = ["Y", "N"]
        combinations = product(poss_values, repeat=5)
        columns = ["shared_request_flag", "shared_match_flag", "access_a_ride_flag", "wav_request_flag",
                   "wav_match_flag"]
        rows = [Row(*row) for row in combinations]
        df_dim_flag = self.spark.createDataFrame(rows, columns)
        dim_flag_path = f"s3a://{self.dwh_location}/dim_flag_t"
        (df_dim_flag.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").save(dim_flag_path))
        self.spark.sql(f"""
                         CREATE TABLE IF NOT EXISTS gold.dim_flag_t
                         USING DELTA
                         LOCATION '{dim_flag_path}'
                     """)
        return

    def create_dim_rate_code(self):
        self.spark.sql('CREATE DATABASE IF NOT EXISTS gold;')
        schema = StructType([
            StructField("rate_code_id", IntegerType(), False),
            StructField("rate_code_type", StringType(), False)
        ])
        data = [
            (1, "Standard rate"),
            (2, "JFK"),
            (3, "Newark"),
            (4, "Nassau or Westchester"),
            (5, "Negotiated fare"),
            (6, "Group ride")
        ]

        df_dim_rate_code = self.spark.createDataFrame(data, schema)
        dim_rate_code_path = f"s3a://{self.dwh_location}/dim_rate_code_t"

        (df_dim_rate_code.write.format("delta")
         .option("overwriteSchema", "true")
         .mode("overwrite").save(dim_rate_code_path))
        self.spark.sql(f"""
                   CREATE TABLE IF NOT EXISTS gold.dim_rate_code_t
                   USING DELTA
                   LOCATION '{dim_rate_code_path}'
               """)
        return

    def create_dim_hvfhs_license_num(self):
        self.spark.sql('CREATE DATABASE IF NOT EXISTS gold;')
        schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("hvfhs_license_num", StringType(), False),
            StructField("company", StringType(), False)
        ])
        data = [
            (1, "HV0002", "Juno"),
            (2, "HV0003", "Uber"),
            (3, "HV0004", "Via"),
            (4, "HV0005", "Lyft")
        ]
        df_dim_hvfhs_license = spark.createDataFrame(data, schema)
        dim_hvfhs_license_path = f"s3a://{self.dwh_location}/dim_hvfhs_license_num_t"
        df_dim_hvfhs_license.write.format("delta").mode("overwrite").save(dim_hvfhs_license_path)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS gold.dim_hvfhs_license_num_t
            USING DELTA
            LOCATION '{dim_hvfhs_license_path}'
        """)

    def create_dim_time(self):
        self.spark.sql('CREATE DATABASE IF NOT EXISTS gold;')
        times_df = self.spark.sql("""
            WITH times AS (
                SELECT explode(sequence(to_timestamp('1900-01-01 00:00'), to_timestamp('1900-01-01 23:59'), interval 1 minute)) AS time
            )
            SELECT time
            FROM times
        """)

        # Tạo bảng dim_time
        dim_time = times_df.select(
            date_format("time", "HHmm").cast("int").alias("id"),
            date_format("time", "hh:mm a").alias("time"),
            date_format("time", "hh").alias("hour"),
            date_format("time", "HH:mm").alias("time_24"),
            date_format("time", "kk").alias("hour_24"),
            date_format("time", "a").alias("am_pm")
        )
        dim_time_path = f"{self.dwh_location}/dim_time_t"
        dim_time.write.format("delta") \
            .option("comment", "Time dimension") \
            .option("delta.targetFileSize", "67108864") \
            .mode("overwrite") \
            .save(dim_time_path) \

        spark.sql(f"""
                  CREATE TABLE IF NOT EXISTS gold.dim_time_t
                  USING DELTA
                  LOCATION '{dim_time_path}'
                  OPTIMIZE time ZORDER BY (time)
              """)
