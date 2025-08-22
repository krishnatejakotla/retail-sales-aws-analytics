SELECT
  date_trunc('month', CAST(invoicedate AS timestamp)) AS month,
  SUM(CAST(quantity AS double) * CAST(unitprice AS double)) AS total_revenue
FROM default.retail_sales_cleaned
WHERE quantity IS NOT NULL AND unitprice IS NOT NULL
GROUP BY 1
ORDER BY 1;
