-- Check for Nulls or Duplicates in primary key ( expectation: no results )

SELECT prd_id,
	count(*)
FROM silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is NULL

-- Check for unwanted spaces ( expectation: no results )

SELECT prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- check for nulls or negative numbers
select prd_cost
from bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost is null;

-- Data Standardization & consistency
SELECT distinct prd_line
FROM bronze.crm_prd_info;

-- Check for invalid date orders
SELECT *
FROM silver.crm_prd_info
where prd_end_dt < prd_start_dt

select * from silver.crm_prd_info

-- checks for crm_sales_details

-- Check for Invalid Dates

SELECT 
NULLIF(sls_due_dt,0) sls_due_dt 
FROM bronze.crm_sales_details
where sls_due_dt <= 0  
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101


-- Check for invalid date orders

SELECT * FROM bronze.crm_sales_details 
WHERE sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

SELECT * FROM silver.crm_sales_details 
WHERE sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- check data consistency between sales, quantity and price
-- sales = quantity * price
-- values must not be NULL, zero or negative

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
or sls_sales IS NULL or sls_quantity <=0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price

select * from silver.crm_sales_details


-- checks for erp_cust_az12

-- identify out-of-range dates

SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data standardization & consistency
SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

SELECT DISTINCT gen FROM silver.erp_cust_az12

SELECT * FROM silver.erp_cust_az12

-- checks for erp_cust_az12

-- Data standardization & consistency
SELECT DISTINCT cntry as old_cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry is NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END as cntry
FROM bronze.erp_loc_a101

SELECT DISTINCT cntry
FROM silver.erp_loc_a101

select * from silver.erp_loc_a101

-- checks for erp_px_cat_g1v2
-- check for unwanted spaces

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat)

-- Data standardization & consistency
SELECT DISTINCT 
cat 
FROM bronze.erp_px_cat_g1v2

select * from silver.erp_px_cat_g1v2
