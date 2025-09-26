/* ====================================================================================================
   Script Name   : data_quality_audit_script.sql
   Purpose       : Identify and fix data quality issues in bronze layer tables before loading to silver layer.
                   Includes checks for extra spaces, nulls, blanks, invalid codes, and standardization logic.

   Caution       : This script performs direct SELECT validations and assumes bronze layer is stable.
                   Do not modify source data here—use only for audit, preview, and transformation planning.

   Usage         : Run before executing silver layer ETL inserts.
                   Each section corresponds to a silver table and includes:
                   - Validation queries
                   - Fix logic preview
                   - Comments for silver layer implementation

   Domains       : ERP → Product Category Info, Customer Info, Location Info
                   CRM → Customer Info, Product Info, Sales Details
   Author        : Priyanshu Kumar Upadhyay
==================================================================================================== */

-- ================================================================
-- Bronze Layer Data Quality Audit Script before loading to Silver Layer
-- ================================================================


-- ================================================================
-- SECTION 1: CRM Customer Info — bronze.crm_cust_info
-- ================================================================

-- 1.1 Check for extra spaces in customer name, gender, and marital status
SELECT *
FROM bronze.crm_cust_info
WHERE
    cst_firstname != TRIM(cst_firstname) OR
    cst_lastname != TRIM(cst_lastname) OR
    cst_marital_status != TRIM(cst_marital_status) OR
    cst_gndr != TRIM(cst_gndr);
-- Issue: Unwanted spaces in string fields
-- Resolution: Applied TRIM() during silver layer transformation

-- 1.2 Check for null or invalid gender values
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr IS NULL OR UPPER(TRIM(cst_gndr)) NOT IN ('F', 'M');
-- Issue: Null or non-standard gender codes
-- Resolution: Standardized to 'Female', 'Male', or 'n/a'

-- 1.3 Check for null or invalid marital status values
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status IS NULL OR UPPER(TRIM(cst_marital_status)) NOT IN ('S', 'M');
-- Issue: Null or non-standard marital status codes
-- Resolution: Standardized to 'Single', 'Married', or 'n/a'

-- 1.4 Check for duplicate customer records based on cst_id
SELECT cst_id, COUNT(*) AS duplicate_count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;
-- Issue: Multiple records per customer
-- Resolution: Used ROW_NUMBER() to retain latest record per cst_id

-- 1.5 Final verification after fixes (optional preview before silver load)
SELECT
    cst_id,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
         ELSE 'n/a' END AS cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         ELSE 'n/a' END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) temp
WHERE flag_last = 1;

-- Verification
SELECT * FROM silver.crm_cust_info;


-- ================================================================
-- SECTION 2: CRM Product Info — bronze.crm_prd_info
-- ================================================================

-- 2.1 Check for malformed product keys
SELECT prd_key
FROM bronze.crm_prd_info
WHERE LEN(prd_key) < 7 OR prd_key NOT LIKE '%-%';
-- Issue: Inconsistent prd_key format
-- Resolution: Split prd_key into cat_id and cleaned suffix using SUBSTRING and REPLACE

-- 2.2 Check for null or invalid product line codes
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info
WHERE prd_line IS NULL OR UPPER(TRIM(prd_line)) NOT IN ('M', 'R', 'S', 'T');
-- Issue: Null or non-standard product line codes
-- Resolution: Standardized to 'Mountain', 'River', 'Other Sales', 'Touring', or 'n/a'

-- 2.3 Check for missing product cost values
SELECT *
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL;
-- Issue: Null cost values
-- Resolution: Replaced with ISNULL(prd_cost, 0)

-- 2.4 Check for missing product start dates
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt IS NULL;
-- Issue: Missing start dates
-- Resolution: Casted to DATE and used LEAD() to derive prd_end_date

-- 2.5 Check for duplicate product records based on prd_id
SELECT prd_id, COUNT(*) AS duplicate_count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;
-- Issue: Multiple records per product
-- Resolution: Used ROW_NUMBER() to retain latest record per prd_id

-- 2.6 Final verification after fixes (optional preview before silver load)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    TRIM(prd_nm) AS prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
         WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'River'
         WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
         WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
         ELSE 'n/a' END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_date,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY prd_id ORDER BY prd_start_dt DESC) AS flag_latest
    FROM bronze.crm_prd_info
) temp
WHERE flag_latest = 1;

-- Verification
SELECT * FROM silver.crm_prd_info;

-- ================================================================
-- SECTION 3: CRM Sales Info — bronze.crm_sales_details
-- ================================================================

-- 3.1 Found invalid order dates
SELECT sls_ord_num, sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt = 0 OR LEN(sls_order_dt) != 8;
-- Issue: Order date is zero or not in YYYYMMDD format
-- Resolution: Converted to NULL and casted to DATE in silver layer

-- 3.2 Found invalid ship dates
SELECT sls_ord_num, sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8;
-- Issue: Ship date is zero or not in YYYYMMDD format
-- Resolution: Converted to NULL and casted to DATE in silver layer

-- 3.3 Found invalid due dates
SELECT sls_ord_num, sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt = 0 OR LEN(sls_due_dt) != 8;
-- Issue: Due date is zero or not in YYYYMMDD format
-- Resolution: Converted to NULL and casted to DATE in silver layer

-- 3.4 Found inconsistent sales values
SELECT sls_ord_num, sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_sales <= 0
   OR sls_sales != sls_quantity * ABS(sls_price);
-- Issue: Sales amount is missing, negative, or doesn't match quantity × price
-- Resolution: Recalculated as quantity × ABS(price) in silver layer

-- 3.5 Found invalid price values
SELECT sls_ord_num, sls_price
FROM bronze.crm_sales_details
WHERE sls_price IS NULL OR sls_price <= 0;
-- Issue: Price is missing or non-positive
-- Resolution: Derived as sales ÷ quantity in silver layer

-- 3.6 Final verification after fixes (optional preview before silver load)
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price END AS sls_price
FROM bronze.crm_sales_details;

-- Verification
SELECT * FROM silver.crm_sales_details;


-- ================================================================
-- SECTION 4: ERP Customer Info — bronze.erp_cust_az12
-- ================================================================

-- 4.1 Found customer IDs with 'NAS' prefix
SELECT cid
FROM bronze.erp_cust_az12
WHERE cid LIKE 'NAS%';
-- Issue: Customer ID contains unwanted 'NAS' prefix
-- Resolution: Removed using SUBSTRING in silver layer

-- 4.2 Found future birthdates
SELECT cid, bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE();
-- Issue: Birthdate is in the future
-- Resolution: Replaced with NULL in silver layer

-- 4.3 Found invalid or non-standard gender values
SELECT DISTINCT gen
FROM bronze.erp_cust_az12
WHERE UPPER(TRIM(gen)) NOT IN ('F', 'FEMALE', 'M', 'MALE');
-- Issue: Gender is null, unknown, or non-standard
-- Resolution: Standardized to 'Female', 'Male', or 'n/a' in silver layer

-- 4.4 Final verification after fixes (optional preview before silver load)
SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a' END AS gen
FROM bronze.erp_cust_az12;

-- Verification
SELECT * FROM silver.erp_cust_az12;

-- ================================================================
-- SECTION 5: ERP Location Info — bronze.erp_loc_a101
-- ================================================================

-- 5.1 Found customer IDs containing hyphens
SELECT cid
FROM bronze.erp_loc_a101
WHERE cid LIKE '%-%';
-- Issue: Customer ID contains hyphens
-- Resolution: Removed using REPLACE(cid, '-', '') in silver layer

-- 5.2 Found country codes that need standardization
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
WHERE TRIM(cntry) IN ('DE', 'us', 'usa');
-- Issue: Country codes are abbreviated
-- Resolution: Mapped 'DE' to 'Germany', 'us'/'usa' to 'United States'

-- 5.3 Found blank or null country values
SELECT cid, cntry
FROM bronze.erp_loc_a101
WHERE TRIM(cntry) = '' OR cntry IS NULL;
-- Issue: Country is blank or missing
-- Resolution: Replaced with 'n/a' in silver layer

-- 5.4 Final verification after fixes (optional preview before silver load)
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('us', 'usa') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;

-- Verification
SELECT * FROM silver.erp_loc_a101;

-- ================================================================
-- SECTION 6: ERP Product Category Info — bronze.erp_px_cat_g1v2
-- ================================================================

-- 6.1 Check for extra spaces in category and subcategory
SELECT id, cat, subcat
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat);
-- Fix: Use TRIM() in silver layer

-- 6.2 Check for missing category or subcategory
SELECT id, cat, subcat
FROM bronze.erp_px_cat_g1v2
WHERE cat IS NULL OR TRIM(cat) = '' OR subcat IS NULL OR TRIM(subcat) = '';
-- Fix: Replace with 'n/a' in silver layer

-- 6.3 Check for invalid maintenance values
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2
WHERE UPPER(TRIM(maintenance)) NOT IN ('YES', 'NO') AND (TRIM(maintenance) IS NOT NULL AND TRIM(maintenance) != '');
-- Fix: Replace with 'n/a' in silver layer

-- 6.4 Check for blank or null maintenance values
SELECT id, maintenance
FROM bronze.erp_px_cat_g1v2
WHERE TRIM(maintenance) = '' OR maintenance IS NULL;
-- Fix: Replace with 'n/a' in silver layer

-- 6.5 Final verification after fixes (optional preview before silver load)
SELECT
	id,
	ISNULL(NULLIF(TRIM(cat), ''), 'n/a') AS cat,
	ISNULL(NULLIF(TRIM(subcat), ''), 'n/a') AS subcat,
	CASE 
		WHEN UPPER(TRIM(maintenance)) = 'YES' THEN 'Yes'
		WHEN UPPER(TRIM(maintenance)) = 'NO' THEN 'No'
		WHEN TRIM(maintenance) IS NULL OR TRIM(maintenance) = '' THEN 'n/a'
		ELSE 'n/a'
	END AS maintenance
FROM bronze.erp_px_cat_g1v2;

-- Verification
SELECT * FROM silver.erp_px_cat_g1v2;
