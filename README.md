# **etl-sql-data-warehouse**  
_A Data Warehouse and Analytics Project_

## Overview
Welcome to the Data Warehouse and Analytics Project repository. This project demonstrates the design and implementation of a modern data warehouse using SQL Server, with a strong emphasis on ETL workflows, schema modeling, and performance-aware data ingestion. It serves as a practical, hands-on showcase of industry best practices in data engineering and analytics.

## Architecture
The project follows a layered architecture:

- **Bronze Layer**: Raw ingestion from CSV files into staging tables.
- **Silver Layer**: Cleaned and standardized data with type casting, code normalization, and audit columns.
- **Gold Layer (planned)**: Analytics-ready dimensional models.

## Current Progress

### Bronze Layer
- Defined six raw ingestion tables across CRM and ERP domains:
  - **CRM**: Customer Info, Product Info, Sales Details
  - **ERP**: Customer Info, Location Info, Product Category Info
- Implemented the `bronze.load_bronze` stored procedure:
  - Truncates and reloads bronze tables using **BULK INSERT**.
  - Tracks duration per table and total batch time.
  - Includes structured print statements for transparency.

### Silver Layer
- Created all silver layer tables with appropriate data types, naming conventions, and `dwh_create_date` audit columns.
- Developed the `silver.load_silver` stored procedure:
  - Truncates silver tables before each load.
  - Applies transformation logic, including:
    - Trimming extra spaces.
    - Replacing nulls and blanks with `'n/a'`.
    - Standardizing codes (e.g., gender, marital status, product line, maintenance flags).
    - Converting integer date fields to proper **DATE** format.
  - Tracks duration per table and full batch.
  - Includes structured print statements and **TRY...CATCH** error handling.

### Validation and Audit Scripts
- Created standalone audit scripts for each silver-bound table.
- Validations include:
  - Extra spaces.
  - Missing or malformed values.
  - Invalid codes and flags.
- Each section includes preview logic and comments for silver layer fixes.

## Next Steps
- Begin gold layer dimensional modeling for analytics-ready schema.
- Introduce slowly changing dimensions and surrogate key logic.
- Package audit scripts into reusable modules for each domain.

## Requirements
- **SQL Server 2019** or later.
- **SSMS** or **Azure Data Studio**.
- **CSV files** for initial data load.

This project is open for both **educational** and **professional** use. Attribution is appreciated :)

