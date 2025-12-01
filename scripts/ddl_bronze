--full load
create or alter procedure bronze.load_bronze as
begin

	declare @start_time datetime, @end_time datetime,@batch_start_time datetime,@batch_end_time datetime
	set @batch_start_time=GETDATE()
	BEGIN TRY
	print '====================================================='
	print 'loading bronze layer...'
	print '====================================================='
	
	set @start_time=GETDATE()
	print '-----------------------------------------------------'
	print 'Loading CRM tables'
	print '-----------------------------------------------------'
	truncate table bronze.crm_cust_info;
	bulk insert bronze.crm_cust_info
	from  'C:\Users\srava\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time=GETDATE()
	PRINT '>> Load Duration: ' +cast(DATEDIFF(second,@start_time, @end_time) as nvarchar) + ' seconds'; 
	PRINT '----------------------------------'

	set @start_time=GETDATE()
	truncate table bronze.crm_prd_info;
	bulk insert bronze.crm_prd_info
	from  'C:\Users\srava\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time=GETDATE()
	PRINT '>> Load Duration: ' +cast(DATEDIFF(second,@start_time, @end_time) as nvarchar) + ' seconds'; 
	PRINT '----------------------------------'

	set @start_time=GETDATE()
	truncate table bronze.crm_sales_details;
	bulk insert bronze.crm_sales_details
	from  'C:\Users\srava\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time=GETDATE()
	PRINT '>> Load Duration: ' +cast(DATEDIFF(second,@start_time, @end_time) as nvarchar) + ' seconds'; 
	PRINT '----------------------------------'

	print '-----------------------------------------------------'
	print 'Loading ERP tables'
	print '-----------------------------------------------------'

	set @start_time=GETDATE()
	truncate table bronze.erp_cust_az12;
	bulk insert bronze.erp_cust_az12
	from  'C:\Users\srava\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time=GETDATE()
	PRINT '>> Load Duration: ' +cast(DATEDIFF(second,@start_time, @end_time) as nvarchar) + ' seconds'; 
	PRINT '----------------------------------'

	set @start_time=GETDATE()
	truncate table bronze.erp_loc_a101;
	bulk insert bronze.erp_loc_a101
	from  'C:\Users\srava\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time=GETDATE()
	PRINT '>> Load Duration: ' +cast(DATEDIFF(second,@start_time, @end_time) as nvarchar) + ' seconds'; 
	PRINT '----------------------------------'

	set @start_time=GETDATE()
	truncate table bronze.erp_px_cat_g1v2;
	bulk insert bronze.erp_px_cat_g1v2
	from  'C:\Users\srava\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time=GETDATE()
	PRINT '>> Load Duration: ' +cast(DATEDIFF(second,@start_time, @end_time) as nvarchar) + ' seconds'; 
	PRINT '----------------------------------'
	end try
	begin catch
		print 'ERROR OCCURED DURING BRONZE LOAD'
		PRINT ERROR_MESSAGE()
		PRINT CAST(ERROR_NUMBER() AS VARCHAR)
		PRINT CAST(ERROR_STATE() AS VARCHAR)
	end catch
	set @batch_end_time=GETDATE()
	PRINT '>> Load Duration: ' +cast(DATEDIFF(second,@batch_start_time, @batch_end_time) as nvarchar) + ' seconds'; 
	PRINT '----------------------------------'
end


