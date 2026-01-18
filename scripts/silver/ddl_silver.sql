/*
===========================================================================
SILVER LAYER TABLE DEFINITION SCRIPT
===========================================================================

Purpose:
  Defines and prepares all Silver layer tables that will store cleansed, standardized,
  and business-ready data derived from the Bronze layer.

What this script does:

  1. Drops existing Silver tables if they already exist to ensure a clean state.
  2. Creates structured Silver tables with well-defined schemas suitable for analytics.
  3. Standardizes data types and column naming across CRM and ERP datasets.
  4. Introduces a consistent auditing column (dwh_create_date) across all tables
     to track when records are created in the Silver layer.
  5. Organizes table creation into two logical sections:
      * CRM tables
      * ERP tables
  6. Lays the foundation for further transformations, validations, and modeling
     in downstream layers (Gold / Reporting).

Tables created in this script:
  - silver.crm_cust_info
  - silver.crm_prd_info
  - silver.crm_sales_details
  - silver.erp_PX_CAT_G1V2
  - silver.erp_LOC_A101
  - silver.erp_CUST_AZ12
*/


IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
    DROP TABLE silver.crm_cust_info 
CREATE TABLE silver.crm_cust_info (
    cst_id INT ,
    cst_key NVARCHAR(30),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR (50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT  ,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
    DROP TABLE  silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARchAR(50) ,
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt NVARCHAR(50),
    sls_ship_dt NVARCHAR(50),
    sls_due_dt NVARCHAR(50),
    sls_sales INT ,
    sls_quantity INT ,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()

)

IF OBJECT_ID('silver.erp_PX_CAT_G1V2','U') IS NOT NULL
    DROP TABLE silver.erp_PX_CAT_G1V2;
CREATE TABLE silver.erp_PX_CAT_G1V2(
    ID NVARCHAR(50)  ,
    CAT NVARCHAR(50),
    SUBCAT NVARCHAR(50),
    MAINTENANCE NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID('silver.erp_LOC_A101','U') IS NOT NULL
    DROP TABLE silver.erp_LOC_A101;
CREATE TABLE silver.erp_LOC_A101(
    CID NVARCHAR(50),
    CNTRY NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID('silver.erp_CUST_AZ12','U') IS NOT NULL
    DROP TABLE silver.erp_CUST_AZ12;
CREATE TABLE silver.erp_CUST_AZ12(
    CID NVARCHAR(50) ,
    BDATE DATE,
    GEN NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)
