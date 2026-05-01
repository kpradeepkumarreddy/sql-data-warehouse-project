/*
================================================================================
    Create Databases
=================================================================================
Script Purpose:
    This script creates databases for each layer: bronze, silver, gold, and also creates a database for storing logs after checking if they already exists. 
    
WARNING:
    Running this script will drop all the databases if they exist. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

DROP DATABASE IF EXISTS datawarehouse_bronze;
CREATE DATABASE datawarehouse_bronze;

DROP DATABASE IF EXISTS datawarehouse_silver;
CREATE DATABASE datawarehouse_silver;

DROP DATABASE IF EXISTS datawarehouse_gold;
CREATE DATABASE datawarehouse_gold;

DROP DATABASE IF EXISTS datawarehouse_logs;
CREATE DATABASE datawarehouse_logs;



USE datawarehouse_logs;

DROP TABLE IF EXISTS load_log;
CREATE TABLE load_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(100),              -- groups one full run
    table_name VARCHAR(100),           -- table being loaded
    load_start_time DATETIME,
    load_end_time DATETIME,
    duration_seconds INT,
    rows_loaded INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
