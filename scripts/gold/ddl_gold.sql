/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/
---------------------------------------------------
-- Create Dimension: gold.dim_products
---------------------------------------------------
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
create view gold.dim_customers as
select 
row_number () over(order by cst_id) as key_number, 
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as firstname,
ci.cst_lastname as lastname,
case when ci.cst_gndr!= 'n/a' then ci.cst_gndr
	 else coalesce(ca.gen,'n/a')
end as gender,
ci.cst_material_status as marital_status,
la.cntry as country,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca 
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la 
on ci.cst_key=la.cid
--------------------------------------------------
-- Create Dimension: gold.dim_products
---------------------------------------------------
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
create view gold.dim_products as
select 
ROW_NUMBER() over(order by pn.prd_start_dt,pn.prd_id) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subccategory,
pc.maintenance,
pn.prd_cost as product_cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where pn.prd_end_dt is null
---------------------------------------------------
-- Create Fact Table: gold.fact_sales
---------------------------------------------------
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
create view gold.dim_sales as
select 
sd.sls_ord_num as  order_number,
pr.product_key ,
cu.key_number,
sd.sls_cust_id as customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_quantity as quantity,
sd.sls_sales as sales
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id=cu.customer_id
