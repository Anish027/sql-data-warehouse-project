/*
=========================================================
SILVER LAYER DATA PROCESS PROCEDURE: silver.load_silver
=========================================================

Purpose:
  Automates end-to-end transformation and refinement of data from Bronze to Silver layer.

What this procedure does:
  1. Reads cleansed and validated data from all Bronze layer tables.
  2. Applies data quality rules, standardization, and business transformations.
  3. Deduplicates records and resolves key data inconsistencies.
  4. Loads transformed, structured, and analytics-ready data into Silver tables.
  5. Tracks start and end time for each table process and prints duration.
  6. Organizes processing in two sections:
      * CRM tables
      * ERP tables
  7. Uses TRY...CATCH for error handling and logs any failures encountered.
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
   DECLARE @start_time DATETIME , @end_time DATETIME2
   BEGIN TRY
--===========================================================================--
            PRINT '=======================================';
            PRINT 'Loading Silver Layer';
            PRINT '=======================================';

            PRINT '---------------------------------------';
            PRINT 'Loading CRM Tables';
            PRINT '---------------------------------------';
   SET @start_time =GETDATE()        
   PRINT '»Truncating Table: silver.crm_cust_info';
   TRUNCATE TABLE silver.crm_cust_info
   PRINT '» Inserting Data Into:silver.crm_cust_info'
   INSERT INTO silver.crm_cust_info(
      cst_id,
      cst_key,
      cst_firstname,
      cst_lastname,
      cst_marital_status,
      cst_gndr,
      cst_create_date)
      
   SELECT 
         cst_id,
         cst_key,
         TRIM(cst_firstname) cst_firstname,
         TRIM(cst_lastname) cst_lastname,
         CASE 
               WHEN TRIM(UPPER(cst_marital_status))='M' THEN 'Married'
               WHEN TRIM(UPPER(cst_marital_status))='S' THEN 'Single'
               ELSE 'n/a'
         END cst_marital_status,
         CASE 
               WHEN TRIM(UPPER(cst_gndr))='M' THEN 'Female'
               WHEN TRIM(UPPER(cst_gndr))='F' THEN 'Male'
               ELSE 'n/a'
         END cst_gndr,
         cst_create_date
      FROM(
         SELECT 
         *,
         ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC ) flag
         FROM bronze.crm_cust_info
         WHERE cst_id IS NOT NULL)t WHERE flag =1
      SET @end_time =GETDATE()
      PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
      PRINT'------------------'  
--===========================================================================--
   SET @start_time =GETDATE()  
   IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
      DROP TABLE silver.crm_prd_info;
   CREATE TABLE silver.crm_prd_info (
      prd_id INT ,
      prd_key NVARCHAR(50),
      cat_ID NVARCHAR(50),
      prd_nm NVARCHAR(50),
      prd_cost INT,
      prd_line NVARCHAR(50),
      prd_start_dt DATE,
      prd_end_dt DATE,
      dwh_create_date DATETIME2 DEFAULT GETDATE()
   )

   --=========================--
   PRINT '»Truncating Table: silver.crm_prd_info';
   TRUNCATE TABLE silver.crm_prd_info
   PRINT '» Inserting Data Into:silver.crm_prd_info'
   INSERT INTO silver.crm_prd_info(
      prd_id,
      cat_ID,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt)
   SELECT 
      prd_id,
      SUBSTRING(REPLACE(TRIM(UPPER(prd_key)),'-','_'),1,5) cat_ID,
      SUBSTRING(REPLACE(TRIM(UPPER(prd_key)),'-','_'),7,LEN(prd_key)) prd_key,
      TRIM(UPPER(prd_pm)) prd_nm,
      ISNULL(prd_cost,0) prd_cost,
      CASE UPPER(TRIM(prd_line))
         WHEN 'M' THEN 'Mountain'
         WHEN 'R' THEN 'Road'
         WHEN 'S' THEN 'Other Sales'
         WHEN 'T' THEN 'Touring'
         ELSE 'n/a'
      END prd_line,
      CAST(prd_start_dt AS DATE) prd_start_dt,
      CAST(LEAD(prd_start_dt)  OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) prd_end_dt
   FROM bronze.crm_prd_info 
   SET @end_time =GETDATE() 
   PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
   PRINT'------------------'
--===========================================================================--
   SET @start_time =GETDATE()  
   IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
      DROP TABLE  silver.crm_sales_details;
   CREATE TABLE silver.crm_sales_details (
      sls_ord_num NVARCHAR(50) ,
      sls_prd_key NVARCHAR(50),
      sls_cust_id INT,
      sls_order_dt DATE,
      sls_ship_dt DATE,
      sls_due_dt DATE,
      sls_sales INT ,
      sls_quantity INT ,
      sls_price INT,
      dwh_create_date DATETIME2 DEFAULT GETDATE()
   )
   --=========================--

   
   PRINT '»Truncating Table: silver.crm_sales_details';
   TRUNCATE TABLE silver.crm_sales_details
   PRINT '» Inserting Data Into:silver.crm_sales_details'
   INSERT INTO silver.crm_sales_details(
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      sls_order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price)

   SELECT 
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      CASE WHEN sls_order_dt <=0 OR LEN(sls_order_dt) != 8 THEN NULL
      ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
      END sls_order_dt,

      CASE WHEN sls_ship_dt <=0 OR LEN(sls_ship_dt) != 8 THEN NULL
      ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
      END sls_ship_dt,

      CASE WHEN sls_due_dt <=0 OR LEN(sls_due_dt) != 8 THEN NULL
      ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
      END sls_due_dt,
      
      CASE WHEN sls_sales != sls_quantity*ABS(sls_price) OR sls_sales<=0 OR sls_sales IS NULL 
            THEN  sls_quantity*ABS(sls_price)
            ELSE sls_sales
      END sls_sales,
      sls_quantity,
      CASE 
         WHEN sls_price < 0 
         THEN ABS(sls_price)

         WHEN sls_price = 0 OR sls_price IS NULL 
         THEN sls_sales / NULLIF(sls_quantity, 0)

         ELSE sls_price
      END AS sls_price
   FROM bronze.crm_sales_details
   SET @end_time =GETDATE() 
   PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
   PRINT'------------------'
--===========================================================================--
   PRINT '---------------------------------------';
   PRINT 'Loading ERP Tables';
   PRINT '---------------------------------------';
   SET @start_time =GETDATE()  
   PRINT '»Truncating Table: silver.erp_CUST_AZ12';
   TRUNCATE TABLE silver.erp_CUST_AZ12
   PRINT '» Inserting Data Into:silver.erp_CUST_AZ12'
   INSERT INTO silver.erp_CUST_AZ12(
      CID,
      BDATE,
      GEN)
   SELECT 
      CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
      ELSE CID
      END CID,
      
      CASE WHEN BDATE <= '1924-01-01' OR BDATE >= GETDATE() THEN NULL
      ELSE BDATE
      END BDATE,
      
      CASE 
         WHEN UPPER(
               REPLACE(
                  REPLACE(
                     REPLACE(TRIM(GEN), CHAR(13), ''),
                  CHAR(10), ''),
               CHAR(9), '')
         ) IN ('M','MALE') THEN 'Male'

         WHEN UPPER(
               REPLACE(
                  REPLACE(
                     REPLACE(TRIM(GEN), CHAR(13), ''),
                  CHAR(10), ''),
               CHAR(9), '')
         ) IN ('F','FEMALE') THEN 'Female'

         ELSE 'N/A'
      END AS GEN_CLEAN
   FROM bronze.erp_CUST_AZ12 
   SET @end_time =GETDATE() 
   PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
   PRINT'------------------'
--===========================================================================--
   SET @start_time =GETDATE()  
   PRINT '»Truncating Table: silver.erp_LOC_A101';
   TRUNCATE TABLE silver.erp_LOC_A101
   PRINT '» Inserting Data Into:silver.erp_LOC_A101'
   INSERT INTO silver.erp_LOC_A101(
      CID,
      CNTRY)

   SELECT 
      REPLACE(CID,'-','') CID,

      CASE 
         WHEN UPPER(REPLACE(REPLACE(REPLACE(TRIM(CNTRY),CHAR(13),''),CHAR(10),''),CHAR(9),''))  = 'DE' THEN 'Germany'

         WHEN UPPER(REPLACE(REPLACE(REPLACE(TRIM(CNTRY),CHAR(13),''),CHAR(10),''),CHAR(9),'')) IN ('US','USA','United States') THEN 'United States'

         WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL 
         THEN 'n/a'

         ELSE UPPER(REPLACE(REPLACE(REPLACE(TRIM(CNTRY),CHAR(13),''),CHAR(10),''),CHAR(9),'')) 
      END AS CNTRY

   FROM bronze.erp_LOC_A101
   SET @end_time =GETDATE() 
   PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
   PRINT'------------------'


--===========================================================================--
   SET @start_time =GETDATE()  
   PRINT '»Truncating Table: silver.erp_PX_CAT_G1V2 ';
   TRUNCATE TABLE silver.erp_PX_CAT_G1V2 
   PRINT '» Inserting Data Into:silver.erp_PX_CAT_G1V2 '
   INSERT INTO silver.erp_PX_CAT_G1V2 (
      ID,
      CAT,
      SUBCAT,
      MAINTENANCE
      )
   SELECT 
      ID,
      CAR,
      SUBCAT,
      UPPER(REPLACE(REPLACE(REPLACE(TRIM(MAINTENANCE),CHAR(13),''),CHAR(10),''),CHAR(9),'')) MAINTENANCE
   FROM bronze.erp_PX_CAT_G1V2 
   SET @end_time =GETDATE() 
   PRINT'>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
   PRINT'------------------' 
   END TRY 
   
   BEGIN CATCH 
      PRINT '=======================================';
      PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER' 
      PRINT 'Error Message' + ERROR_MESSAGE () ;
      PRINT 'Error Message' + CAST(ERROR_NUMBER()AS NVARCHAR);
      PRINT '======================================='; 
   END CATCH
END


