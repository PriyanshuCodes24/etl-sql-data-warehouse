/* ====================================================================================================
   Script Name   : ddl_loadgold.sql
   Purpose       : Define gold layer views for dimensional modeling and analytics-ready fact tables.
                   Includes surrogate keys, standardized attributes, and referential joins to silver layer.

   Caution       : These views assume silver layer tables are fully loaded and validated.
                   Surrogate keys are generated using ROW_NUMBER() for dimensional consistency.

   Objects       : 
                   - gold.dimension_customers
                   - gold.dimension_products
                   - gold.fact_sales

   Usage         : These views support reporting, dashboarding, and analytical queries.
                   Join facts to dimensions using surrogate keys for performance and clarity.
   Author        : Priyanshu Kumar Upadhyay
==================================================================================================== */

USE DataWarehouse;
GO

-- ============================================================================================
-- View Name     : gold.dimension_customers
-- Purpose       : Create a customer dimension for the gold layer using CRM as master source.
-- Notes         : 
--   - Surrogate key generated via ROW_NUMBER()
--   - CRM is treated as authoritative for gender and identity
--   - ERP tables supplement birthdate and country info
-- ============================================================================================

CREATE VIEW gold.dimension_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,         -- Surrogate Key
    ci.cst_id AS customer_id,                                       -- Internal CRM ID
    ci.cst_key AS customer_number,                                  -- External customer reference
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;
GO

-- ============================================================================================
-- View Name     : gold.dimension_products
-- Purpose       : Create a product dimension for the gold layer by combining CRM and ERP sources.
-- Notes         : 
--   - Surrogate key generated via ROW_NUMBER()
--   - CRM is treated as authoritative for product identity and cost
--   - ERP product category info enriches category and maintenance attributes
--   - Historical records (with non-null end dates) are excluded
-- ============================================================================================

CREATE VIEW gold.dimension_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_date, pn.prd_key) AS product_key, -- Surrogate Key
    pn.prd_id AS product_id,                   -- Internal CRM product ID
    pn.prd_key AS product_number,              -- External product reference
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,                  -- Derived from prd_key in silver layer
    pc.cat AS category,                        -- From ERP category table
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,             -- Standardized to Yes/No/n/a
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,               -- Standardized product line (Mountain, River, etc.)
    pn.prd_start_date AS start_date            -- Only active products retained
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE pn.prd_end_date IS NULL;
GO

-- ============================================================================================
-- View Name     : gold.fact_sales
-- Purpose       : Create a sales fact view for the gold layer using surrogate keys from dimensions.
-- Notes         : 
--   - Uses surrogate keys from dimension views for referential integrity
--   - Filters and joins are based on cleaned silver layer data
--   - Grain: One row per sales order line item
-- ============================================================================================

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,              -- Sales order number from CRM
    pr.product_key,                              -- From gold.dimension_products
    cu.customer_key,                             -- From gold.dimension_customers
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dimension_products pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dimension_customers cu ON sd.sls_cust_id = cu.customer_id;
GO
