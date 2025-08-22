# Retail Sales Analytics (AWS S3 → Glue → Athena → Power BI)

This project demonstrates an **end-to-end analytics pipeline** built on AWS and connected to Power BI for visualization.  
It showcases **data engineering + BI practices** by ingesting raw retail data into S3, cleaning it with Glue, querying with Athena, and visualizing insights in Power BI via ODBC.

---

## 🚀 Architecture

**Pipeline Flow**
- Raw CSVs stored in **S3** (`s3://<your-bucket>/raw/`)
- **AWS Glue Job** cleans, casts, and filters data
- Processed Parquet written back to **S3** (`s3://<your-bucket>/processed/retail_sales_cleaned/`)
- Registered in **Glue Data Catalog**
- Queried with **Athena SQL**
- Visualized in **Power BI via ODBC**

**Project Structure**
- `README.md`  
- `.gitignore`  
- `glue/retail_sales_cleaning_job.py`  
- `athena/00_table_ddl_retail_sales_cleaned.sql`  
- `athena/10_monthly_revenue.sql`  
- `athena/20_top_countries.sql`  
- `athena/30_top_customers.sql`  
- `athena/40_top_products.sql`  
- `docs/odbc_powerbi_setup.md`  
- `assets/architecture.png`  
- `assets/dashboard_overview.png`  
- `assets/monthly_revenue.png`  
- `assets/top_countries.png`  
- `powerbi/Retail_Sales_Analysis.pbix`  

---

## 🔑 Key Features

- **AWS S3** → storage for raw + processed data  
- **AWS Glue** → ETL job to clean data, cast datatypes, drop invalids, write Parquet  
- **Amazon Athena** → serverless SQL queries over S3 data (cheaper with Parquet)  
- **Power BI** → live dashboards connected via ODBC  

---

## 📊 Queries Implemented

- Monthly revenue trend  
- Top countries by revenue  
- Top customers by revenue  
- Top products by revenue  

---

## ✅ Data Quality Rules

- Dropped rows with null/blank `customerid`  
- Cast `quantity` and `unitprice` to numeric  
- Removed rows with negative quantities or prices  
- Renamed columns to standard schema  

---

## 📊 Power BI Dashboards

Screenshots (see `assets/`):  
- Monthly Revenue Trend  
- Top Countries by Revenue  
- Top Customers by Revenue  
- Top Products by Revenue  

---

## ⚙️ Setup Instructions

1. **Ingest Raw Data**  
   Upload your CSV to: `s3://<your-bucket>/raw/`

2. **Run Glue Job**  
   Script: `glue/retail_sales_cleaning_job.py`  
   Output written to: `s3://<your-bucket>/processed/retail_sales_cleaned/` (Parquet)

3. **Create Athena Table**  
   Run: `athena/00_table_ddl_retail_sales_cleaned.sql`

4. **Validate with Queries**  
   Run queries in `athena/` folder (monthly revenue, top countries, etc.)

5. **Connect Power BI**  
   Follow `docs/odbc_powerbi_setup.md`:  
   - Install Simba Athena ODBC Driver  
   - Configure DSN → Region `us-east-2`, Workgroup `primary`, Catalog `AwsDataCatalog`, S3 output path  
   - Connect Power BI → ODBC → select `default.retail_sales_cleaned`

---

## 🔒 Security

- Do not commit AWS credentials, keys, or account IDs  
- Use AWS profiles or IAM roles for authentication  
- Keep S3 buckets private; block public access  

---

## 💰 Cost Optimization

- Store processed data in **Parquet** → reduces Athena query scan cost  
- Partition data if dataset grows  
- Delete old query results from S3 periodically  

---

## 📈 Resume Impact

- Built an **end-to-end cloud analytics pipeline** (S3 → Glue → Athena → Power BI)  
- Delivered insights: monthly revenue, top countries, customers, products  
- Implemented **data quality filters + schema enforcement**  
- Automated refresh via ODBC live connection  
