# Initialize SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, BooleanType
import pandas as pd

import holidays as pyholidays

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config(f'spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog') \
    .config(f'spark.sql.catalog.glue_catalog.warehouse', "s3://aegon-demo-datalake/") \
    .config(f'spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog') \
    .config(f'spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO') \
    .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .getOrCreate()
    
# 1) Read Silver Iceberg table
silver_df = spark.read.table("glue_catalog.aegon_demo_silver.credit_card_transactions")

# 2) Extract date
silver_df = silver_df.withColumn("date", F.to_date(F.col("trans_date_trans_time")))

# 3) Aggregate to daily summary
daily = silver_df.groupBy("date").agg(F.sum(F.col("is_fraud")).alias("fraud_count"), F.count(F.col("cc_num")).alias("total_transactions")).withColumn( "fraud_rate", F.round( F.col("fraud_count")/F.col("total_transactions"), 4))

# 4) Weekend & holiday flags
# Weekend flag (Spark dayofweek: 1=Sun, 7=Sat)
daily = (daily
         .withColumn("dow", F.dayofweek("date"))
         .withColumn("is_weekend", F.col("dow").isin(1, 7).cast(BooleanType()))
        )

bounds = daily.select(F.min("date").alias("min_d"), F.max("date").alias("max_d")).first()
years = list(range(bounds.min_d.year, bounds.max_d.year + 1))
hset = pyholidays.country_holidays("US", subdiv=None, years=years)

holiday_dates = pd.DataFrame({
    "date": [pd.to_datetime(d).date() for d in hset.keys()],
    "is_holiday": True
})

holidays_df = spark.createDataFrame(holiday_dates) \
                .withColumn("date", F.col("date").cast(DateType()))

daily = (daily.join(holidays_df, on="date", how="left")
            .withColumn("is_holiday", F.coalesce(F.col("is_holiday"), F.lit(False))))

# Final select (ordered)
daily_gold_df = (daily
    .select("date", "total_transactions", "fraud_count", "fraud_rate",
            "dow", "is_weekend", "is_holiday")
    .orderBy("date")
)

# Step 5: Write to Iceberg table in Gold layer (create or replace)
gold_table_path = "glue_catalog.aegon_demo_gold.credit_card_fraud_daily_summary"

daily_gold_df.writeTo(gold_table_path) \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

print(f"Gold Iceberg table '{gold_table_path}' created/updated.")
