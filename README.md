# etl-sql-data-warehouse — "A Data Warehouse and Analytics Project"

Welcome to the **Data Warehouse and Analytics Project** repository!

This project showcases the design and implementation of a modern data warehouse using **SQL Server**, with a strong focus on ETL workflows, schema modeling, and performance-aware data ingestion. It’s built as a practical, hands-on demonstration of industry best practices in data engineering and analytics.

---

## ✅ Current Progress

- **Bronze Layer Schema Design**  
  Defined six raw ingestion tables across CRM and ERP domains, including customer info, product data, sales details, location, demographics, and product categories.

- **Bulk Data Loading Procedures**  
  Implemented a robust stored procedure (`bronze.load_bronze`) to truncate and load all bronze tables from CSV files using `BULK INSERT`.

- **Granular Duration Tracking**  
  Each table load is timed individually, and total batch duration is calculated in both seconds and minutes for auditability and performance monitoring.

- **Structured Print Statements**  
  Clear, stepwise console output for every ETL action—truncation, insertion, and timing—ensures transparency and easy debugging.

---

## 🛠️ Next Steps

- Build **Silver Layer** with transformations and type casting (e.g., converting `INT` date fields to `DATE`)
- Add **row count validation** and error logging
- Implement **metadata tracking** for load status and duration
- Extend to **Gold Layer** for analytics-ready dimensional modeling

---

Stay tuned as this project evolves into a full-stack data warehouse solution with audit trails, performance metrics, and scalable ETL architecture.
