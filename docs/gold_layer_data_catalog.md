# Gold Layer Data Catalog  
**Project**: etl-sql-data-warehouse  
**Layer**: Gold (Analytics-Ready)  
**Author**: Priyanshu Kumar Upadhyay
**Last Updated**: 2025-09-27

---

## Overview

This catalog documents all dimension and fact views defined in the gold layer of the data warehouse. Each entry includes surrogate key definitions, grain, source mappings, and usage notes. These views are designed for analytics, reporting, and BI integration.

---

## Dimension: `gold.dimension_customers`

**Grain**: One row per unique customer  
**Surrogate Key**: `customer_key` generated via `ROW_NUMBER()`  

| Column Name        | Description                                      | Source Table(s)               |
|--------------------|--------------------------------------------------|-------------------------------|
| `customer_key`      | Surrogate key for dimensional joins              | silver.crm_cust_info          |
| `customer_id`       | CRM internal ID                                  | silver.crm_cust_info          |
| `customer_number`   | External customer reference                      | silver.crm_cust_info          |
| `first_name`        | Customer first name                              | silver.crm_cust_info          |
| `last_name`         | Customer last name                               | silver.crm_cust_info          |
| `country`           | Country from ERP location info                   | silver.erp_loc_a101           |
| `marital_status`    | Standardized marital status                      | silver.crm_cust_info          |
| `gender`            | CRM gender preferred, fallback to ERP           | silver.crm_cust_info + erp_cust_az12 |
| `birthdate`         | Birthdate from ERP                               | silver.erp_cust_az12          |
| `create_date`       | CRM customer creation date                       | silver.crm_cust_info          |

**Usage**: Joins with `gold.fact_sales` via `customer_key`

---

## Dimension: `gold.dimension_products`

**Grain**: One row per active product (filtered by `prd_end_date IS NULL`)  
**Surrogate Key**: `product_key` generated via `ROW_NUMBER()`  

| Column Name        | Description                                      | Source Table(s)               |
|--------------------|--------------------------------------------------|-------------------------------|
| `product_key`       | Surrogate key for dimensional joins              | silver.crm_prd_info           |
| `product_id`        | CRM internal product ID                          | silver.crm_prd_info           |
| `product_number`    | External product reference                       | silver.crm_prd_info           |
| `product_name`      | Product name                                     | silver.crm_prd_info           |
| `category_id`       | Derived from `prd_key`                           | silver.crm_prd_info           |
| `category`          | ERP product category                             | silver.erp_px_cat_g1v2        |
| `subcategory`       | ERP product subcategory                          | silver.erp_px_cat_g1v2        |
| `maintenance`       | Maintenance flag (Yes/No/n/a)                    | silver.erp_px_cat_g1v2        |
| `cost`              | Product cost                                     | silver.crm_prd_info           |
| `product_line`      | Standardized product line                        | silver.crm_prd_info           |
| `start_date`        | Product start date                               | silver.crm_prd_info           |

**Usage**: Joins with `gold.fact_sales` via `product_key`

---

## Fact: `gold.fact_sales`

**Grain**: One row per sales order line item  
**Surrogate Keys Used**: `product_key`, `customer_key`  

| Column Name        | Description                                      | Source Table(s)               |
|--------------------|--------------------------------------------------|-------------------------------|
| `order_number`      | Sales order number                               | silver.crm_sales_details      |
| `product_key`       | Surrogate key from `dimension_products`          | gold.dimension_products       |
| `customer_key`      | Surrogate key from `dimension_customers`         | gold.dimension_customers      |
| `order_date`        | Date of order                                    | silver.crm_sales_details      |
| `shipping_date`     | Date of shipment                                 | silver.crm_sales_details      |
| `due_date`          | Due date for delivery                            | silver.crm_sales_details      |
| `sales_amount`      | Total sales amount                               | silver.crm_sales_details      |
| `quantity`          | Quantity sold                                    | silver.crm_sales_details      |
| `price`             | Unit price                                       | silver.crm_sales_details      |

**Usage**: Central fact table for sales analysis, joins with customer and product dimensions

---

## Notes

- All surrogate keys are generated using `ROW_NUMBER()` for simplicity and consistency.
- Dimensions are designed to be slowly changing dimension-ready, though SCD logic is not yet implemented.
- Fact views are non-aggregated and represent base-level transactional data.
- All views assume silver layer tables are fully validated and transformed.

---
