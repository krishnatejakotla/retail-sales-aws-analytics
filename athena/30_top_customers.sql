SELECT
  CustomerID,
  SUM(Quantity * UnitPrice) AS revenue
FROM default.retail_sales_cleaned
GROUP BY CustomerID
ORDER BY revenue DESC
LIMIT 10;
