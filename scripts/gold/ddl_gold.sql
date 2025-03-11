-- Create dimension Products

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers as
SELECT
ROW_NUMBER() OVER (ORDER BY cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
CASE WHEN ci.cst_gndr ! = 'n/a' THEN ci.cst_gndr -- CRM is master for gender info
	 else coalesce(ca.gen, 'n/a')
end as gender,
ci.cst_create_date as create_date,
ca.bdate as birthdate
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid


-- we have information about genter from two sources, we need to do data integrarion

SELECT distinct
ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr ! = 'n/a' THEN ci.cst_gndr -- CRM is master for gender info
	 else coalesce(ca.gen, 'n/a')
end as new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
order by 1,2


-- Create Dimension Products

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
	
CREATE VIEW gold.dim_products AS
SELECT
row_number() over(order by pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as product_category,
pc.subcat as product_subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL --Filter out all historical data


-- Create Fact Sales

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
	
CREATE VIEW gold.fact_sales as 
SELECT 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

-- foreign key integrity (dimensions)

select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
where c.customer_key is null

select * from gold.fact_sales f
left join gold.dim_customers c
ON c.customer_key = f.customer_key
left join gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key is NULL
