CREATE EXTERNAL TABLE IF NOT EXISTS default.retail_sales_cleaned (
  InvoiceNo string,
  StockCode string,
  Description string,
  Quantity int,
  InvoiceDate string,
  UnitPrice double,
  CustomerID string,
  Country string
)
STORED AS PARQUET
LOCATION 's3://retail-sales-data-teja/processed/';
