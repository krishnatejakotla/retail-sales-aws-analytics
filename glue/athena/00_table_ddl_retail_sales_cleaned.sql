CREATE EXTERNAL TABLE IF NOT EXISTS default.retail_sales_cleaned (
  invoiceno   string,
  stockcode   string,
  description string,
  quantity    int,
  invoicedate string,
  unitprice   double,
  customerid  string,
  country     string
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/processed/retail_sales_cleaned/';
