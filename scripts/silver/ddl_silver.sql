/* ====================================================================================================
   Script Name   : ddl_silver.sql
   Purpose       : Define and initialize silver layer tables for CRM and ERP domains in DataWarehouse.
                   These tables store cleaned, standardized, and audit-ready data from bronze layer sources.
   Caution       : This script drops and recreates silver layer tables. All existing data will be lost.
                   Ensure backups or downstream dependencies are handled before execution.
   Domains       : CRM → Customer Info, Product Info, Sales Details
                   ERP → Customer Info, Location Info, Product Category Info
   Author        : Priyanshu Kumar Upadhyay
==================================================================================================== */

USE DataWarehouse;

-- ================================================================
-- CRM Section: Silver Layer Table Definitions
-- ================================================================

-- Drop and Create: CRM Customer Info
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Drop and Create: CRM Product Info
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id NVARCHAR(10),               -- Extracted from prd_key
    prd_key NVARCHAR(50),              -- Suffix part of original prd_key
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_date DATE,               -- Renamed for clarity
    prd_end_date DATE,                 -- Renamed for clarity
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Drop and Create: CRM Sales Details
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,                 -- Changed the datatype to DATE
    sls_ship_dt DATE,                  -- Changed the datatype to DATE
    sls_due_dt DATE,                   -- Changed the datatype to DATE
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- ================================================================
-- ERP Section: Silver Layer Table Definitions
-- ================================================================

-- Drop and Create: ERP Location Info
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Drop and Create: ERP Customer Info
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Drop and Create: ERP Product Category Info
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
