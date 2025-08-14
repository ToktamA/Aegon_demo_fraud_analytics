import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth

# Glue job args
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Iceberg Config - Do not modify spark.sql.warehouse.dir!
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://aegon-demo-datalake/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

# Read CSV from Bronze
bronze_path = "s3://aegon-demo-datalake/bronze/credit_card_transactions/credit_card_transactions.csv"
df = spark.read.option("header", "true").csv(bronze_path)

# Clean & cast data
df_clean = df.withColumn("trans_date_trans_time", to_timestamp(col("trans_date_trans_time"))) \
             .withColumn("amt", col("amt").cast("float")) \
             .withColumn("is_fraud", col("is_fraud").cast("int")) \
             .withColumn("unix_time", col("unix_time").cast("long")) \
             .drop("Unnamed: 0") \
             .dropna(subset=["trans_date_trans_time", "cc_num", "amt", "merchant", "category"])

# Add partition columns
df_final = df_clean \
    .withColumn("year", year(col("trans_date_trans_time"))) \
    .withColumn("month", month(col("trans_date_trans_time"))) \
    .withColumn("day", dayofmonth(col("trans_date_trans_time")))

# Write to Iceberg
df_final.writeTo("glue_catalog.aegon_demo_silver.credit_card_transactions") \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .partitionedBy("year", "month", "day") \
    .createOrReplace()

# Commit job
job.commit()
