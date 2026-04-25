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


-- transform and load into datawarehouse_silver.crm_cust_info
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

