SELECT
  customerid,
  SUM(CAST(quantity AS double) * CAST(unitprice AS double)) AS total_revenue
FROM default.retail_sales_cleaned
WHERE customerid IS NOT NULL
  AND TRIM(CAST(customerid AS varchar)) <> ''
  AND quantity IS NOT NULL AND unitprice IS NOT NULL
GROUP BY customerid
ORDER BY total_revenue DESC
LIMIT 10;
