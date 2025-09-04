exec bronze.load_bronze

create or alter procedure bronze.load_bronze as 
begin
declare @start_time Datetime, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME
  begin try
	SET @batch_start = GETDATE()
	print '====================';
	print 'loading bronze layer';
	print '====================';
	print 'loading crm tables ';
	set @start_time = GETDATE();
	truncate table bronze.crm_cust_info;
	bulk insert bronze.crm_cust_info
	from 'C:\Users\asus\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	print '>> Laod Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
	print '>>------------------'

	set @start_time = GETDATE();
	truncate table bronze.crm_prd_info;

	bulk insert bronze.crm_prd_info
	from 'C:\Users\asus\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	print '>> Laod Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'


	set @end_time = GETDATE();
	truncate table bronze.crm_sales_details;

	bulk insert bronze.crm_sales_details
	from 'C:\Users\asus\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	print '>> Laod Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

	print '====================';
	print 'loading bronze layer';
	print '====================';
	print 'loading erp tables ';

	set @end_time = GETDATE();
	truncate table bronze.erp_loc_a101;

	bulk insert bronze.erp_loc_a101
	from 'C:\Users\asus\Documents\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	
	set @end_time = GETDATE();
	truncate table bronze.crm_sales_details;

	set @end_time = GETDATE();
	truncate table bronze.erp_cust_az12;

	bulk insert bronze.erp_cust_az12
	from 'C:\Users\asus\Documents\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	truncate table bronze.crm_sales_details;

	set @start_time = GETDATE();
	truncate table bronze.erp_px_cat_g1v2;

	bulk insert bronze.erp_px_cat_g1v2
	from 'C:\Users\asus\Documents\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	truncate table bronze.crm_sales_details;
	SET @batch_end = GETDATE()
	print '=================='
	print '- batch time bronze layer' + CAST(DATEDIFF(second, @batch_start, @batch_end) as nvarchar) + 'seconds';
	print '=================='
  end try
  begin catch
	print '=================='
	print 'error occured in bronze layer';
	print 'error message' + ERROR_MESSAGE();
	print 'error message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	print '=================='
  end catch
end
