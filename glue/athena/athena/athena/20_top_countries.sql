SELECT
  country,
  SUM(CAST(quantity AS double) * CAST(unitprice AS double)) AS total_revenue
FROM default.retail_sales_cleaned
WHERE quantity IS NOT NULL AND unitprice IS NOT NULL
GROUP BY country
ORDER BY total_revenue DESC
LIMIT 10;
