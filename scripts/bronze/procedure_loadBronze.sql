/*
====================================================================
Script: Procedure to Load Bronze Layer (Source > Bronze)
------------------------------------------------------------
Purpose: Loads raw data from CRM and ERP source CSV files into 
         Bronze Layer tables in the DataWarehouse. Each table is 
         truncated before loading, and duration metrics are printed 
         for individual tables and the full batch.

Usage:
    - Ensure all CSV files are present at the specified paths.
    - Run this script in the context of the DataWarehouse database.
    - This procedure is intended for development or controlled batch 
      ingestion environments.

Warning:
    - This script will truncate all bronze tables before loading.
    - File paths are hardcoded and must be accessible to SQL Server.
    - Do not run in production without validating file access and 
      backup requirements.

Author: Priyanshu Kumar Upadhyay
====================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

    SET @batch_start_time = GETDATE();

    BEGIN TRY
        PRINT '========================================================'
        PRINT 'Starting Bronze Layer Load...'
        PRINT '========================================================'

        PRINT '--------------------------------------------------------'
        PRINT 'Loading CRM Tables...'
        PRINT '--------------------------------------------------------'

        -- 1. Customer Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting: bronze.crm_cust_info'
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
        PRINT '---------------------------'

        -- 2. Product Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.crm_prd_info'
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting: bronze.crm_prd_info'
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
        PRINT '---------------------------'

        -- 3. Sales Details
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting: bronze.crm_sales_details'
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
        PRINT '---------------------------'

        PRINT '--------------------------------------------------------'
        PRINT 'Loading ERP Tables...'
        PRINT '--------------------------------------------------------'

        -- 4. Location Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting: bronze.erp_loc_a101'
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
        PRINT '---------------------------'

        -- 5. Customer Demographics
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting: bronze.erp_cust_az12'
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
        PRINT '---------------------------'

        -- 6. Product Category
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.erp_px_cat_g1v2'
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting: bronze.erp_px_cat_g1v2'
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
        PRINT '---------------------------'

        -- Final Batch Duration
        SET @batch_end_time = GETDATE();
        PRINT '========================================================'
        PRINT 'Bronze Layer Load Completed.'
        PRINT 'Total Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds'
        PRINT 'Total Batch Duration: ' + CAST(DATEDIFF(MINUTE, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Minutes'
        PRINT '========================================================'

    END TRY
    BEGIN CATCH
        PRINT '========================================================'
        PRINT 'ERROR OCCURRED DURING LOADING THE BRONZE LAYER'
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '========================================================'
    END CATCH
END;
GO

-- Execute the procedure to load bronze layer
EXEC bronze.load_bronze;
