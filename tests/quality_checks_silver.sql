-- Check for Nulls or Duplicates in Primary Key
SELECT cst_id, COUNT(*) FROM datawarehouse_bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id is NULL;

-- Check for unwanted spaces
SELECT cst_firstname FROM datawarehouse_bronze.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname);
SELECT cst_lastname FROM datawarehouse_bronze.crm_cust_info WHERE cst_lastname != TRIM(cst_lastname);
SELECT cst_gndr FROM datawarehouse_bronze.crm_cust_info WHERE cst_gndr != TRIM(cst_gndr);

-- Data Standardization and consistency
SELECT DISTINCT cst_gndr FROM datawarehouse_bronze.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM datawarehouse_bronze.crm_cust_info;

SELECT * FROM datawarehouse_bronze.crm_cust_info;

describe datawarehouse_bronze.crm_cust_info;

SELECT COUNT(*) 
FROM datawarehouse_bronze.crm_cust_info
WHERE cst_gndr='';



-- data cleaning crm_prd_info table
SELECT * FROM datawarehouse_bronze.crm_prd_info;

-- Check for Nulls or Duplicates in Primary Key
SELECT prd_id, count(*) FROM datawarehouse_bronze.crm_prd_info GROUP BY prd_id HAVING count(*) > 1 OR prd_id IS NULL;

SELECT * FROM datawarehouse_bronze.erp_px_cat_g1v2;

-- check for unwanted spaces
SELECT * FROM datawarehouse_bronze.crm_prd_info WHERE prd_nm != TRIM(prd_nm);
DESCRIBE datawarehouse_bronze.crm_prd_info;


-- check for negative or null values in prd_cost
SELECT * FROM datawarehouse_bronze.crm_prd_info WHERE prd_cost <0 OR prd_cost IS NULL;

-- Data standardization and consistency
SELECT DISTINCT prd_line FROM datawarehouse_bronze.crm_prd_info;
SELECT  * FROM datawarehouse_bronze.crm_prd_info WHERE prd_line != TRIM(prd_line);

SELECT DISTINCT TRIM(prd_line) FROM datawarehouse_bronze.crm_prd_info;



-- data cleaning crm_sales_details table
SELECT * from datawarehouse_bronze.crm_sales_details;
DESCRIBE datawarehouse_bronze.crm_sales_details;

SELECT * FROM datawarehouse_bronze.crm_sales_details WHERE sls_ord_num != TRIM(sls_ord_num);

-- check any issues in joining keys 'sls_prd_key' and 'sls_cust_id'
SELECT 
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id 
	sls_order_dt,
	sls_ship_dt ,
	sls_due_dt ,
	sls_sales ,
	sls_quantity,
	sls_price
FROM datawarehouse_bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM datawarehouse_silver.crm_prd_info);

SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id, 
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM datawarehouse_bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM datawarehouse_silver.crm_cust_info);


-- Check for invalid dates
SELECT 
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM datawarehouse_bronze.crm_sales_details
WHERE (sls_order_dt IS NULL OR sls_order_dt=0) OR (sls_ship_dt IS NULL OR sls_ship_dt=0) OR (sls_due_dt IS NULL OR sls_due_dt=0);


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


-- data cleaning crm_sales_details table
SELECT * from datawarehouse_bronze.erp_cust_az12;
DESCRIBE datawarehouse_bronze.erp_cust_az12;

SELECT cid FROM datawarehouse_bronze.erp_cust_az12 LIMIT 5;

SELECT cst_key FROM datawarehouse_silver.crm_cust_info LIMIT 5;


-- check for invalid bdate
SELECT DISTINCT bdate FROM datawarehouse_bronze.erp_cust_az12 WHERE bdate < '1926-01-01' OR bdate > NOW();


-- Data Standardization and Consistency
SELECT DISTINCT gen FROM datawarehouse_bronze.erp_cust_az12;
SELECT DISTINCT
	CASE 
		WHEN UPPER(TRIM(REPLACE(gen,'\r',''))) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(REPLACE(gen,'\r',''))) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'n/a'
    END AS gen 
FROM datawarehouse_bronze.erp_cust_az12;

SELECT * FROM datawarehouse_bronze.erp_cust_az12 WHERE TRIM(gen) != gen;
SELECT UPPER(TRIM('Male '))='MALE';

SELECT gen, LENGTH(gen), HEX(gen)
FROM datawarehouse_bronze.erp_cust_az12;

SELECT cid, cntry FROM datawarehouse_bronze.erp_loc_a101;
DESCRIBE datawarehouse_bronze.erp_loc_a101;

SELECT REPLACE(cid,'-', ''), cntry FROM datawarehouse_bronze.erp_loc_a101;
SELECT DISTINCT cntry FROM datawarehouse_bronze.erp_loc_a101;

-- Data Standardization and consistency
SELECT DISTINCT
	CASE
		WHEN UPPER(TRIM(REPLACE(cntry,'\r',''))) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(cntry,'\r','')) = 'DE' THEN 'Germnay'
        WHEN cntry IS NULL OR TRIM(cntry) ='\r' THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry, cntry    
FROM datawarehouse_bronze.erp_loc_a101;

SELECT cntry, LENGTH(cntry), HEX(cntry)
FROM datawarehouse_bronze.erp_loc_a101;
SELECT cntry FROM datawarehouse_bronze.erp_loc_a101 WHERE cntry = '';


SELECT * FROM datawarehouse_bronze.erp_px_cat_g1v2;
DESCRIBE datawarehouse_bronze.erp_px_cat_g1v2;

SELECT id,  LENGTH(id), cat, LENGTH(cat), subcat, LENGTH(subcat), maintenance, LENGTH(maintenance) FROM datawarehouse_bronze.erp_px_cat_g1v2;
SELECT id FROM datawarehouse_bronze.erp_px_cat_g1v2 WHERE id LIKE '%\r%';
SELECT cat FROM datawarehouse_bronze.erp_px_cat_g1v2 WHERE cat LIKE '%\r%';
SELECT subcat FROM datawarehouse_bronze.erp_px_cat_g1v2 WHERE subcat LIKE '%\r%';
SELECT maintenance FROM datawarehouse_bronze.erp_px_cat_g1v2 WHERE maintenance LIKE '%\r%';

SELECT DISTINCT REPLACE(maintenance, '\r', '') AS maintenance FROM datawarehouse_bronze.erp_px_cat_g1v2;
