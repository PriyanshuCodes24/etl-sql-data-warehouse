/* ============================================================================
   Script Name   : Create_DataWarehouse.sql
   Purpose       : 
     This script creates a new SQL Server database named 'DataWarehouse'
     and defines three organizational schemas within it: bronze, silver, and gold.
     
     - bronze: Stores raw and ingested data as-is from various source systems.
     - silver: Contains cleansed, transformed, and standardized data.
     - gold  : Stores aggregated and business-ready data for reporting and analytics.
     
     This layered schema approach supports the ELT (Extract, Load, Transform) 
     or Medallion Architecture in modern data warehousing.
   Author        : Priyanshu Kumar Upadhyay
   Environment   : SQL Server
============================================================================ */

-- Step 1: Switch to the master database (default system database)
USE master;
GO

-- Step 2: Create the DataWarehouse database
CREATE DATABASE DataWarehouse;
GO

-- Step 3: Switch to the newly created DataWarehouse database
USE DataWarehouse;
GO

-- ==========================================
-- Creating Schemas for Data Organization
-- ==========================================
-- A schema in SQL is like a container or folder.
-- It helps organize the database objects (tables, views, procedures, etc.) logically.

-- 1st Schema: Bronze Schema (Raw data)
-- Used for staging and storing raw, unprocessed data from source systems.
CREATE SCHEMA bronze;
GO

-- 2nd Schema: Silver Schema (Transformed and cleansed data)
-- Contains standardized, cleaned, and enriched data.
CREATE SCHEMA silver;
GO

-- 3rd Schema: Gold Schema (Business-ready and aggregated data)
-- Stores curated, analytics-ready data for reporting and dashboards.
CREATE SCHEMA gold;
GO


