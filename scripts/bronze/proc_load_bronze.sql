/*
=====================================================
BRONZE LAYER DATA LOAD PROCEDURE: bronze.load_bronze
=====================================================

Purpose:
    Automates end-to-end loading of all Bronze layer tables from raw CSV files.

What this procedure does:

    1. Truncates existing data in all Bronze CRM and ERP tables.
    2. Bulk loads fresh raw data from source CSV files into corresponding tables.
    3. Tracks start and end time for each table load and prints duration.
    4. Organizes loading in two sections:
        * CRM tables
        * ERP tables
    5. Uses TRY...CATCH for error handling and logs any failures encountered.


*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
    BEGIN 
        DECLARE @start_time DATETIME, @end_time DATETIME;
        BEGIN TRY
            PRINT '=======================================';
            PRINT 'Loading Bronze Layer';
            PRINT '=======================================';

            PRINT '---------------------------------------';
            PRINT 'Loading CRM Tables';
            PRINT '---------------------------------------';
            
            SET @start_time = GETDATE();
            PRINT '»Truncating Table: bronze.crm_cust_info';
            TRUNCATE TABLE  bronze.crm_cust_info;
            
            PRINT '» Inserting Data Into:bronze.crm_cust_info'
            BULK INSERT bronze.crm_cust_info
            FROM  '/var/opt/mssql/data/datasets/source_crm/cust_info.csv'
            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
            SET @end_time = GETDATE();

            PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
            PRINT'------------------'
            
            SET @start_time = GETDATE();
            PRINT '»Truncating Table: bronze.crm_prd_info';
            TRUNCATE TABLE bronze.crm_prd_info;
            
            PRINT '» Inserting Data Into: bronze.crm_prd_info'
            BULK INSERT bronze.crm_prd_info
            FROM '/var/opt/mssql/data/datasets/source_crm/prd_info.csv'
            WITH(
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK 
            );
            SET @end_time = GETDATE();

            PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
            PRINT'------------------'

            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_sales_details; 
            PRINT '» Inserting Data Into: bronze.crm_sales_details'
            
            BULK INSERT bronze.crm_sales_details
            FROM '/var/opt/mssql/data/datasets/source_crm/sales_details.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR =',',
                TABLOCK 
            );
            SET @end_time = GETDATE();

            PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
            PRINT'------------------'

            PRINT '---------------------------------------';
            PRINT 'Loading ERP Tables';
            PRINT '---------------------------------------';
            
            SET @start_time = GETDATE();
            PRINT '»Truncating Table: bronze.erp_CUST_AZ12';
            TRUNCATE TABLE bronze.erp_CUST_AZ12;
            
            PRINT '» Inserting Data Into: bronze.erp_CUST_AZ12'
            BULK INSERT bronze.erp_CUST_AZ12
            FROM '/var/opt/mssql/data/datasets/source_erp/CUST_AZ12.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR =',',
                TABLOCK 
            );
            SET @end_time = GETDATE();

            PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
            PRINT'------------------'

            SET @start_time = GETDATE();
            PRINT '»Truncating Table: bronze.erp_LOC_A101';
            TRUNCATE TABLE bronze.erp_LOC_A101;
            
            PRINT '» Inserting Data Into: bronze.erp_LOC_A101'
            BULK INSERT bronze.erp_LOC_A101
            FROM '/var/opt/mssql/data/datasets/source_erp/LOC_A101.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR =',',
                TABLOCK 
            );
            SET @end_time = GETDATE();

            PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
            PRINT'------------------'

            SET @start_time = GETDATE();
            PRINT '»Truncating Table: bronze.erp_PX_CAT_G1V2';
            TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
        
            PRINT '» Inserting Data Into: bronze.erp_PX_CAT_G1V2'
            BULK INSERT bronze.erp_PX_CAT_G1V2
            FROM '/var/opt/mssql/data/datasets/source_erp/PX_CAT_G1V2.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR =',',
                TABLOCK 
            );
            SET @end_time = GETDATE();

            PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
            PRINT'------------------'
            END TRY 
        
        BEGIN CATCH
            PRINT '=======================================';
            PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER' 
            PRINT 'Error Message' + ERROR_MESSAGE () ;
            PRINT 'Error Message' + CAST(ERROR_NUMBER()AS NVARCHAR);
            PRINT '======================================='; 
        END CATCH 
END




