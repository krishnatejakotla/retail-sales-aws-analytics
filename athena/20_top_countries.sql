SELECT
  Country,
  SUM(Quantity * UnitPrice) AS revenue
FROM default.retail_sales_cleaned
GROUP BY Country
ORDER BY revenue DESC
LIMIT 10;
