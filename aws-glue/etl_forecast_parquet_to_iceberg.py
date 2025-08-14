import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

# --- Spark Session for Iceberg ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp

spark = (
    SparkSession.builder
    .appName("ParquetToIceberg_Forecast")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://aegon-demo-datalake/")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.defaultCatalog", "glue_catalog")
    .getOrCreate()
)

# --- Parameters ---
S3_PARQUET_PATH = "s3://aegon-demo-datalake/_staging/credit_card_fraud_rate_daily_forecast/"
ICEBERG_DB = "aegon_demo_gold"
ICEBERG_TABLE = "credit_card_fraud_rate_daily_forecast"

# --- Step 1: Read Parquet ---
df = spark.read.parquet(S3_PARQUET_PATH)

# --- Step 2: Rename columns and add run metadata ---
df_transformed = (
    df
    .withColumnRenamed("ds", "forecast_date")
    .withColumnRenamed("yhat", "forecast_rate")
    .withColumnRenamed("yhat_lower", "forecast_lower")
    .withColumnRenamed("yhat_upper", "forecast_upper")
    .withColumnRenamed("yhat_clipped", "forecast_rate_clipped")
    .withColumn("run_date", current_date())
    .withColumn("run_time_utc", current_timestamp())
)

# --- Step 3: Create Iceberg Table ---
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{ICEBERG_DB}.{ICEBERG_TABLE} (
        forecast_date DATE,
        forecast_rate DOUBLE,
        forecast_lower DOUBLE,
        forecast_upper DOUBLE,
        forecast_rate_clipped DOUBLE,
        run_date DATE,
        run_time_utc TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (run_date)
""")

# --- Step 4: Write to Iceberg (overwrite for now) ---
(
    df_transformed
    .writeTo(f"glue_catalog.{ICEBERG_DB}.{ICEBERG_TABLE}")
    .overwritePartitions()
)

print(f"Iceberg table glue_catalog.{ICEBERG_DB}.{ICEBERG_TABLE} created/updated successfully.")