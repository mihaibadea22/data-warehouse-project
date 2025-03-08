
/*
===============================================================================
Stored Procedure: Load Silver Layer (
===============================================================================
Description:
    This stored procedure executes the ETL (Extract, Transform, Load) process  
    to populate tables in the 'silver' schema using cleansed and transformed 
    data from the 'bronze' schema.

Operations:
    - Truncates existing Silver tables.
    - Loads transformed data from Bronze into Silver tables.
    - Logs execution times and errors.

Parameters:
    None. The procedure does not accept parameters or return values.

Usage:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	-- LOADING DATA INTO silver.crm_cust_info TABLE
	PRINT '>>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>>> Inserting data into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info (
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
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		case when UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			 when UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			 else 'n/a'
		end cst_material_status,
		case when UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 when UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 else 'n/a'
		end cst_gndr,
		cst_create_date
	FROM (
	SELECT *,
	row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
	FROM bronze.crm_cust_info
	where cst_id is not null
	) t
	WHERE flag_last = 1


	-- Loading data into silver.crm_prd_info table
	PRINT '>>> Truncating Table: silver.crm_prd_info table';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>>> Inserting data into: silver.crm_prd_info table';
	INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	SUBSTRING(PRD_KEY, 7, LEN(prd_key)) as prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	case when UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 when UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 when UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 when UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 else 'n/a'
		 end as prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) as prd_end_dt
	FROM bronze.crm_prd_info


	-- Loading data into silver.crm_sales_details table
	PRINT '>>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>>> Inserting data into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT sls_ord_num,
		  sls_prd_key,
		  sls_cust_id,
	   case when sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
			else cast(cast(sls_order_dt as VARCHAR) as DATE)
		END AS sls_order_dt,
	   case when sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
			else cast(cast(sls_ship_dt as VARCHAR) as DATE)
		END AS sls_ship_dt,
		case when sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
		else cast(cast(sls_due_dt as VARCHAR) as DATE)
		END AS sls_due_dt,
			CASE WHEN sls_sales IS NULL or sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				then sls_quantity * ABS(sls_price)
				else sls_sales
			end as sls_sales,
		  sls_quantity,
		case when sls_price is null or sls_price <=0
			then sls_sales / NULLIF(sls_quantity,0)
			else sls_price
		end as sls_price
	FROM bronze.crm_sales_details


	-- Loading data into silver.erp_cust_az12 table
	PRINT '>>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>>> Inserting data into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END as cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12

	-- Loading data into silver.erp_loc_a101 table
	PRINT '>>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>>> Inserting data into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(cid, cntry)
	select
	REPLACE(cid, '-', '') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry is NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END as cntry
	from bronze.erp_loc_a101

	-- Loading data into silver.erp_px_cat_g1v2 table
	PRINT '>>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>>> Inserting data into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2
END