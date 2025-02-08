
/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
---VALUES INSERTION---
--TRUNCATE TABLE--
--BECAUSE YOU RUN THE BULK INSERT TWICE THE DATA WILL AGAIN LOADED SO FIST TRUNCATE TABLE AFTER INSERT VALUES

---TABLE 1---
CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
	DECLARE @START DATETIME, @END DATETIME
	SET @START =GETDATE()
	DECLARE @START_TIME DATETIME, @END_TIME DATETIME
	BEGIN TRY
		PRINT '================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '================================';

		PRINT '--------------------------------';
		PRINT 'LOADING CRM';
		PRINT '--------------------------------';
		SET @START_TIME=GETDATE()
		PRINT 'TRUNCATING TABLE:BRONZE.CRM_CUST_INFO'
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT 'INSERTING TABLE:BRONZE.CRM_CUST_INFO'

		BULK INSERT BRONZE.CRM_CUST_INFO
		FROM 'C:\DATAWAREHOUSE_FILES\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		SET @END_TIME=GETDATE()
		PRINT 'LOADING DURATION: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds';
		PRINT '------------------'
		---TABLE 2---
		SET @START_TIME=GETDATE()

		PRINT 'TRUNCATING TABLE:bronze.crm_prd_info'

		TRUNCATE TABLE bronze.crm_prd_info

		PRINT 'INSERTING TABLE:bronze.crm_prd_info'

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\DATAWAREHOUSE_FILES\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		SET @END_TIME=GETDATE()
		PRINT 'LOADING DURATION: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds';
		PRINT '------------------'

		---TABLE 3---
		SET @START_TIME=GETDATE()

		PRINT 'TRUNCATING TABLE:bronze.crm_sales_details'

		TRUNCATE TABLE bronze.crm_sales_details

		PRINT 'INSERTING TABLE:bronze.crm_sales_details'

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\DATAWAREHOUSE_FILES\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		SET @END_TIME=GETDATE()
		PRINT 'LOADING DURATION: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds';
		PRINT '------------------'

		--TABLE 4---
		SET @START_TIME=GETDATE()

		PRINT '--------------------------------';
		PRINT 'LOADING ERP';
		PRINT '--------------------------------';

		PRINT 'TRUNCATING TABLE:bronze.erp_cust_az12'

		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT 'INSERTING TABLE:bronze.erp_cust_az12 '

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\DATAWAREHOUSE_FILES\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @END_TIME=GETDATE()
		PRINT 'LOADING DURATION: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds';
		PRINT '------------------'

		--TABLE 5--
		SET @START_TIME=GETDATE()

		PRINT 'TRUNCATING TABLE:bronze.erp_loc_a101'

		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT 'INSERTING TABLE:bronze.erp_cust_az12 '

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\DATAWAREHOUSE_FILES\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @END_TIME=GETDATE()
		PRINT 'LOADING DURATION: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds';
		PRINT '------------------'


		--TABLE 6---
		SET @START_TIME=GETDATE()

		PRINT 'TRUNCATING TABLE:bronze.erp_px_cat_g1v2'

		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT 'INSERTING TABLE:bronze.erp_px_cat_g1v2 '

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\DATAWAREHOUSE_FILES\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @END_TIME=GETDATE()
		PRINT 'LOADING DURATION: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds';
		PRINT '------------------'
		SET @END=GETDATE()
		PRINT '===========LOADING BRONZE LAYER IS COMPLETED============'
		PRINT 'TOTAL LOADING DURATION: '+CAST(DATEDIFF(second,@start,@end) as NVARCHAR)+' SECONDS'
	END TRY
	BEGIN CATCH
			PRINT '========================================'
			PRINT 'Error Message'+ error_message();
			PRINT 'Error Message'+ cast(error_number() as nvarchar);
			PRINT 'Error Message'+ cast(error_state() as nvarchar);
			PRINT '======================================='
	END CATCH
END

