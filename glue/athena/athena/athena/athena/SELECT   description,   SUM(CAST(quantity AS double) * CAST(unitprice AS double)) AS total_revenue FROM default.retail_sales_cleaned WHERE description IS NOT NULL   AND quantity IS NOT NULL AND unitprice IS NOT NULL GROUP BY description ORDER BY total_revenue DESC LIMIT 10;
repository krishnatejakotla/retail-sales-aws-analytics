SELECT
  description,
  SUM(CAST(quantity AS double) * CAST(unitprice AS double)) AS total_revenue
FROM default.retail_sales_cleaned
WHERE description IS NOT NULL
  AND quantity IS NOT NULL AND unitprice IS NOT NULL
GROUP BY description
ORDER BY total_revenue DESC
LIMIT 10;
