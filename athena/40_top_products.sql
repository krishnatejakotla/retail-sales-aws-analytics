SELECT
  Description,
  SUM(Quantity * UnitPrice) AS revenue
FROM default.retail_sales_cleaned
GROUP BY Description
ORDER BY revenue DESC
LIMIT 10;
