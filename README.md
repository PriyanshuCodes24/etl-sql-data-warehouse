# ETL-SQL-Data-Warehouse  
**A Data Warehouse and Analytics Project**

---

## 📊 Overview

Welcome to the **ETL-SQL Data Warehouse** project — a full-stack data warehousing solution built on **SQL Server**. This project demonstrates the end-to-end pipeline of **data ingestion**, **cleaning**, **transformation**, and **analytics modeling**, following industry best practices.

Key focuses include:

- 🔄 ETL workflow orchestration  
- 🧱 Multi-layered schema modeling (Bronze, Silver, Gold)  
- 🚀 Performance-aware batch processing  
- 🔍 Auditability and traceability across all layers  

---

## 🧱 Architecture

The project follows a **layered medallion architecture**, designed for modularity, scalability, and analytics readiness:

- **Bronze Layer**: Raw ingestion from CSV files into SQL Server staging tables  
- **Silver Layer**: Cleaned, standardized, and enriched data with audit fields  
- **Gold Layer**: Dimensional models and fact views optimized for BI, reporting, and dashboarding  

---

## ✅ Project Highlights

### 🔹 Bronze Layer

- Six staging tables across **CRM** and **ERP** domains:
  - **CRM**: Customer Info, Product Info, Sales Details  
  - **ERP**: Customer Info, Location Info, Product Category Info  
- `bronze.load_bronze` stored procedure:
  - Performs `BULK INSERT` from CSVs  
  - Truncates and reloads data for full refresh  
  - Tracks execution time per table and batch  
  - Structured logging for transparency  

---

### 🔸 Silver Layer

- All silver tables defined with:
  - Consistent naming conventions  
  - Appropriate data types and constraints  
  - `dwh_create_date` audit columns  
- `silver.load_silver` stored procedure:
  - Truncates and reloads data  
  - Cleans and transforms input, including:
    - Trimming whitespace  
    - Standardizing codes (e.g., gender, marital status, product line)  
    - Handling `NULL`/blank values with `'n/a'`  
    - Converting integer-based dates to SQL `DATE`  
  - Built-in `TRY...CATCH` for error handling  
  - Execution time tracking per table and overall  

---

### 🧪 Data Validation & Audits

- Independent audit scripts per silver table to verify:
  - Extra or trailing spaces  
  - Missing, malformed, or default values  
  - Invalid or unrecognized lookup codes  
- Scripts include:
  - Preview logic  
  - Comments for quick resolution in transformation layer  

---

### 🟡 Gold Layer

- Dimensional and fact views ready for analytics:
  - `gold.dimension_customers`: Combines CRM & ERP customer data  
  - `gold.dimension_products`: Enriches product info with category mapping  
  - `gold.fact_sales`: Sales fact view using surrogate keys and consistent date formats  
- Features:
  - Surrogate keys via `ROW_NUMBER()`  
  - Optimized for Power BI/Tableau/Looker  
  - Grain definitions and referential integrity captured in [Gold Layer Data Catalog](docs/gold_layer_data_catalog.md)

---

## 🛠️ Requirements

To run this project locally or in a dev environment, ensure the following are available:

| Requirement            | Details                                |
|------------------------|----------------------------------------|
| **SQL Server**         | 2019 or later                          |
| **Client Tools**       | SQL Server Management Studio (SSMS) or Azure Data Studio |
| **Data Files**         | CSV files placed in `./datasets/` directory |

---

## 📁 Repository Structure

```plaintext
etl-sql-data-warehouse/
│
├── bronze/
│   ├── ddl_bronze.sql
│   ├── procedure_loadBronze.sql
│
├── silver/
│   ├── ddl_silver.sql
│   ├── data_quality_audit_script.sql
│   ├── procedure_loadSilver.sql
│
├── gold/
│   ├── ddl_loadgold.sql
│
├── docs/
│   ├── gold_layer_data_catalog.md
│
├── datasets/
│   ├── [CSV files for CRM and ERP domains]
│
└── README.md
```
---

📄 License

This project is available for educational and professional use. Attribution is appreciated but not required.

---

🙌 Acknowledgements

This project was developed as part of a personal data engineering portfolio to showcase real-world ETL and data warehouse design patterns using SQL Server :)
