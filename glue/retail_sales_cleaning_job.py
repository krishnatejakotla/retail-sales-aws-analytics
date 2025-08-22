import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---- READ RAW ----
# Raw path: s3://<your-bucket>/raw/
raw_df = spark.read.option("header", "true").csv("s3://<your-bucket>/raw/")

# ---- BASIC CLEANING ----
clean = (
    raw_df
    .withColumnRenamed("InvoiceNo", "invoiceno")
    .withColumnRenamed("StockCode", "stockcode")
    .withColumnRenamed("Description", "description")
    .withColumnRenamed("Quantity", "quantity")
    .withColumnRenamed("InvoiceDate", "invoicedate")
    .withColumnRenamed("UnitPrice", "unitprice")
    .withColumnRenamed("CustomerID", "customerid")
    .withColumnRenamed("Country", "country")
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unitprice", col("unitprice").cast("double"))
    .withColumn("customerid", trim(col("customerid").cast("string")))
    .filter((col("quantity") > 0) & (col("unitprice") > 0) & col("invoiceno").isNotNull())
)

# ---- WRITE PROCESSED PARQUET ----
# Processed path: s3://<your-bucket>/processed/retail_sales_cleaned/
(
    clean
    .repartition(8)
    .write
    .mode("overwrite")
    .parquet("s3://<your-bucket>/processed/retail_sales_cleaned/")
)

job.commit()
