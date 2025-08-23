import sys
from awsglue.transforms import DropNullFields
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

# Step 1: Read raw data from Glue Catalog
dyf = glueContext.create_dynamic_frame.from_catalog(
    database = "default",
    table_name = "retail_sales_raw"
)

# Step 2: Drop null CustomerID
non_null_dyf = dyf.filter(lambda row: row["CustomerID"] is not None)

# Step 3: Cast Quantity and UnitPrice to numeric
casted_dyf = non_null_dyf.resolveChoice(specs=[
    ("Quantity", "cast:int"),
    ("UnitPrice", "cast:double")
])

# Step 4: Remove negative quantities/prices
cleaned_dyf = casted_dyf.filter(lambda row: row["Quantity"] >= 0 and row["UnitPrice"] >= 0)

# Step 5: Write processed data to S3 in Parquet
glueContext.write_dynamic_frame.from_options(
    frame = cleaned_dyf,
    connection_type = "s3",
    connection_options = {"path": "s3://retail-sales-data-teja/processed/"},
    format = "parquet"
)

job.commit()
