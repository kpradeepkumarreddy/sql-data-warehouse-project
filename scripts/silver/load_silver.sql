/*
===============================================================================
             Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' database tables from the 'bronze' database tables.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
===============================================================================
*/

-- transform and load into datawarehouse_silver.crm_cust_info
TRUNCATE datawarehouse_silver.crm_cust_info;
INSERT INTO datawarehouse_silver.crm_cust_info(
	cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
	END AS cst_marital_status,
	CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END AS cst_gndr,
	cst_create_date
FROM
	(SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS cst_id_rank 
	FROM datawarehouse_bronze.crm_cust_info
	WHERE cst_id != 0 AND cst_id IS NOT NULL)t
WHERE cst_id_rank = 1; 


-- transform and load into datawarehouse_silver.crm_prd_info
TRUNCATE datawarehouse_silver.crm_prd_info;
INSERT INTO datawarehouse_silver.crm_prd_info(
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
	REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') AS cat_id,	-- Extract category ID
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,		-- Extract product key
	prd_nm,
	prd_cost,
    CASE UPPER(TRIM(prd_line))
		WHEN  'M' THEN 'Mountain'
        WHEN  'T' THEN 'Touring'
        WHEN  'S' THEN 'Other Sales'
        WHEN  'R' THEN 'Road'
        ELSE 'n/a'
    END AS prd_line,		-- Map product line codes to descriptive values
	DATE(prd_start_dt) AS prd_start_dt,
	DATE(
    DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY)
    ) AS prd_end_dt			-- Calculate end date as one day before the next start date
FROM datawarehouse_bronze.crm_prd_info;


-- transform and load into datawarehouse_silver.crm_sales_details
TRUNCATE datawarehouse_silver.crm_sales_details;
INSERT INTO  datawarehouse_silver.crm_sales_details(
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
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id, 
	CASE 
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS CHAR) AS DATE)
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS CHAR) AS DATE)
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS CHAR) AS DATE)
	END AS sls_due_dt,
    CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price  -- Derive price if original value is invalid
	END AS sls_price
FROM datawarehouse_bronze.crm_sales_details;


-- transform and load into datawarehouse_silver.erp_cust_az12
TRUNCATE datawarehouse_silver.erp_cust_az12;
INSERT INTO datawarehouse_silver.erp_cust_az12(
	cid,
    bdate,
    gen
)   
SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		ELSE cid
	END AS cid,    
	CASE
		WHEN bdate > NOW() THEN NULL	-- convert future birthdates to NULL
		ELSE bdate
    END AS bdate, 
	CASE 
		WHEN UPPER(TRIM(REPLACE(gen,'\r',''))) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(REPLACE(gen,'\r',''))) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'n/a'
    END AS gen 		-- standardize the values
FROM datawarehouse_bronze.erp_cust_az12;
            
            
-- transform and load into datawarehouse_silver.erp_loc_a101
TRUNCATE datawarehouse_silver.erp_loc_a101;
INSERT INTO datawarehouse_silver.erp_loc_a101(
	cid,
    cntry
)
SELECT 
	REPLACE(cid,'-', '') AS cid, 
	CASE
		WHEN UPPER(TRIM(REPLACE(cntry,'\r',''))) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(cntry,'\r','')) = 'DE' THEN 'Germany'
        WHEN cntry IS NULL OR TRIM(cntry) ='\r' THEN 'n/a'
        ELSE TRIM(REPLACE(cntry,'\r',''))
    END AS cntry 
FROM datawarehouse_bronze.erp_loc_a101;


-- transform and load into datawarehouse_silver.erp_px_cat_g1v2
TRUNCATE datawarehouse_silver.erp_px_cat_g1v2;
INSERT INTO datawarehouse_silver.erp_px_cat_g1v2(
	id,
    cat,
    subcat,
	maintenance
)
SELECT id,
		cat,
        subcat,
        REPLACE(maintenance, '\r', '') AS maintenance
FROM datawarehouse_bronze.erp_px_cat_g1v2;
