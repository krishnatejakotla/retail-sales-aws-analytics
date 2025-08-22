# Connect Power BI to Amazon Athena via ODBC (Simba)

1) Install the **Simba Amazon Athena ODBC Driver (64-bit)**.
2) Create a **User/System DSN**:
   - Data Source Name: `Athena_PowerBI`
   - Region: `us-east-2` (Ohio)
   - Workgroup: `primary`
   - S3 Output Location: `s3://<your-bucket>/athena-results/`
3) Authentication:
   - Use AWS Profile (`default`) **or**
   - UID = Access Key ID, PWD = Secret Access Key
4) In Power BI Desktop: Get Data → ODBC → `Athena_PowerBI` → select `default.retail_sales_cleaned`
5) Build visuals (monthly revenue, top countries/customers/products).
