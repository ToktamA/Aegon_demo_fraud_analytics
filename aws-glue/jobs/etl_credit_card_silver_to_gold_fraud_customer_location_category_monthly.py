from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, max, to_date, concat_ws, countDistinct, round as spark_round
)

# Step 1: Start Spark session with Iceberg support
spark = SparkSession.builder \
    .appName("Gold Layer Monthly Summary - Iceberg") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://aegon-demo-datalake/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.defaultCatalog", "glue_catalog") \
    .getOrCreate()

# Step 2: Read Silver Iceberg table
silver_df = spark.read.format("iceberg").load("glue_catalog.aegon_demo_silver.credit_card_transactions")

# Step 3: Prepare derived columns
silver_df = silver_df \
    .withColumn("trans_day", to_date(col("trans_date_trans_time")))

# Step 4: Aggregate to monthly customer summary
gold_df = silver_df.groupBy("cc_num", "state", "merchant", "category", "year", "month").agg(
    count("*").alias("total_transactions"),
    sum("amt").alias("total_spent"),
    avg("amt").alias("avg_transaction_amt"),
    sum("is_fraud").alias("total_fraud_count"),
    max("trans_date_trans_time").alias("last_transaction_time"),
    countDistinct("trans_day").alias("total_active_days")
).withColumn(
    "fraud_rate", spark_round(col("total_fraud_count") / col("total_transactions"), 4)
)

# Step 5: Create the Gold database in Glue if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.aegon_demo_gold")

# Step 6: Define full table name for Iceberg
gold_table = "glue_catalog.aegon_demo_gold.credit_card_fraud_customer_location_category_monthly_summary"

# Step 7: Write to Gold Iceberg table (create or replace)
gold_df.writeTo(gold_table) \
    .using("iceberg") \
    .option("fanout-enabled", "true") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

print(f"Iceberg Gold table '{gold_table}' updated successfully.")
