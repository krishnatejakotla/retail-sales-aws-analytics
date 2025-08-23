SELECT
  date_format(parse_datetime(InvoiceDate, 'yyyy-MM-dd HH:mm:ss'), '%Y-%m') AS month,
  SUM(Quantity * UnitPrice) AS revenue
FROM default.retail_sales_cleaned
GROUP BY 1
ORDER BY 1;
