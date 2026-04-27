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

-- Create dimension 'Customers'
CREATE OR REPLACE VIEW datawarehouse_gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,		-- surrogate key
	ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
	la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr		-- CRM is the Master for gender info
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birth_date,
    ci.cst_create_date AS creation_date
FROM datawarehouse_silver.crm_cust_info ci
LEFT JOIN datawarehouse_silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN datawarehouse_silver.erp_loc_a101 la ON ci.cst_key = la.cid;


-- Create dimension 'Products'
CREATE OR REPLACE VIEW datawarehouse_gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY prd_start_dt, prd_key) AS product_key,
	prd_id AS product_id,			
    prd_key AS product_number,
    prd_nm AS product_name,
    cat_id AS category_id,	
    pc.cat AS category,
    pc.subcat AS sub_category,
    pc.maintenance,
    prd_cost AS cost,
    prd_line AS product_line,
    prd_start_dt AS start_date
FROM datawarehouse_silver.crm_prd_info pn
LEFT JOIN datawarehouse_silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;	-- Filter out historical data

-- Create fact 'Sales'
CREATE OR REPLACE VIEW datawarehouse_gold.fact_sales AS
SELECT 
	cs.sls_ord_num AS order_number,
    pr.product_key,
	cu.customer_key,
	cs.sls_order_dt AS order_date,
	cs.sls_ship_dt AS ship_date,
	cs.sls_due_dt AS due_date,
	cs.sls_sales AS sales,
	cs.sls_quantity AS quantity,
	cs.sls_price AS price
FROM datawarehouse_silver.crm_sales_details cs
LEFT JOIN datawarehouse_gold.dim_customers cu ON cs.sls_cust_id = cu.customer_id
LEFT JOIN datawarehouse_gold.dim_products pr ON cs.sls_prd_key = pr.product_number;
