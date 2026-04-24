
# SHOW VARIABLES LIKE 'local_infile';
# SET GLOBAL local_infile = 1;

SET @batch_id := UUID();

-- crm_cust_info
SET @start_time = NOW();
TRUNCATE TABLE datawarehouse_bronze.crm_cust_info;

LOAD DATA LOCAL INFILE '/Users/pradeep/Downloads/DataWithBaraa/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE datawarehouse_bronze.crm_cust_info
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

SET @end_time = NOW();

SELECT COUNT(*) INTO @row_count FROM datawarehouse_bronze.crm_cust_info;

INSERT INTO datawarehouse_logs.load_log (
	batch_id,
    table_name,
    load_start_time,
    load_end_time,
    duration_seconds,
    rows_loaded
)
VALUES (
	@batch_id,
    'datawarehouse_bronze.crm_cust_info',
    @start_time,
    @end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    @row_count
);


-- crm_prd_info
SET @start_time = NOW();
TRUNCATE TABLE datawarehouse_bronze.crm_prd_info;

LOAD DATA LOCAL INFILE '/Users/pradeep/Downloads/DataWithBaraa/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE datawarehouse_bronze.crm_prd_info
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

SET @end_time = NOW();
SELECT COUNT(*) INTO @row_count FROM datawarehouse_bronze.crm_prd_info;

INSERT INTO datawarehouse_logs.load_log (
	batch_id,
    table_name,
    load_start_time,
    load_end_time,
    duration_seconds,
    rows_loaded
)
VALUES (
	@batch_id,
    'datawarehouse_bronze.crm_prd_info',
    @start_time,
    @end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    @row_count
);

-- crm_sales_details
SET @start_time = NOW();
TRUNCATE TABLE datawarehouse_bronze.crm_sales_details;

LOAD DATA LOCAL INFILE '/Users/pradeep/Downloads/DataWithBaraa/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE datawarehouse_bronze.crm_sales_details
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

SET @end_time = NOW();
SELECT COUNT(*) INTO @row_count FROM datawarehouse_bronze.crm_sales_details;

INSERT INTO datawarehouse_logs.load_log (
	batch_id,
    table_name,
    load_start_time,
    load_end_time,
    duration_seconds,
    rows_loaded
)
VALUES (
	@batch_id,
    'datawarehouse_bronze.crm_sales_details',
    @start_time,
    @end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    @row_count
);


-- ================= ERP TABLES =================

-- erp_loc_a101 (NOTE: filename is uppercase)
SET @start_time = NOW();
TRUNCATE TABLE datawarehouse_bronze.erp_loc_a101;

LOAD DATA LOCAL INFILE '/Users/pradeep/Downloads/DataWithBaraa/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
INTO TABLE datawarehouse_bronze.erp_loc_a101
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

SET @end_time = NOW();
SELECT COUNT(*) INTO @row_count FROM datawarehouse_bronze.erp_loc_a101;

INSERT INTO datawarehouse_logs.load_log (
	batch_id,
    table_name,
    load_start_time,
    load_end_time,
    duration_seconds,
    rows_loaded
)
VALUES (
	@batch_id,
    'datawarehouse_bronze.erp_loc_a101',
    @start_time,
    @end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    @row_count
);

-- erp_cust_az12 (NOTE: filename is uppercase)
SET @start_time = NOW();
TRUNCATE TABLE datawarehouse_bronze.erp_cust_az12;

LOAD DATA LOCAL INFILE '/Users/pradeep/Downloads/DataWithBaraa/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE datawarehouse_bronze.erp_cust_az12
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

SET @end_time = NOW();
SELECT COUNT(*) INTO @row_count FROM datawarehouse_bronze.erp_cust_az12;

INSERT INTO datawarehouse_logs.load_log (
	batch_id,
    table_name,
    load_start_time,
    load_end_time,
    duration_seconds,
    rows_loaded
)
VALUES (
	@batch_id,
    'datawarehouse_bronze.erp_cust_az12',
    @start_time,
    @end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    @row_count
);

-- erp_px_cat_g1v2 (NOTE: filename is uppercase)
SET @start_time = NOW();
TRUNCATE TABLE datawarehouse_bronze.erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE '/Users/pradeep/Downloads/DataWithBaraa/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE datawarehouse_bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

SET @end_time = NOW();
SELECT COUNT(*) INTO @row_count FROM datawarehouse_bronze.erp_px_cat_g1v2;

INSERT INTO datawarehouse_logs.load_log (
	batch_id,
    table_name,
    load_start_time,
    load_end_time,
    duration_seconds,
    rows_loaded
)
VALUES (
	@batch_id,
    'datawarehouse_bronze.erp_px_cat_g1v2',
    @start_time,
    @end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    @row_count
);

SELECT * FROM datawarehouse_logs.load_log where batch_id = @batch_id ORDER BY created_at DESC;
