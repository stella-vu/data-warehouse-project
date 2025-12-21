-- Turn local_infile ON
set global local_infile = 1;
-- Checking
SHOW VARIABLES LIKE 'local_infile';

--Delete all data in table
TRUNCATE TABLE bronze.crm_cust_info;

--Upload data from a local file
LOAD DATA LOCAL INFILE '/Users/stellavu/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Repeat the process to all table

TRUNCATE TABLE bronze.crm_prd_info;
LOAD DATA LOCAL INFILE '/Users/stellavu/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

TRUNCATE TABLE bronze.crm_sales_details;
LOAD DATA LOCAL INFILE '/Users/stellavu/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

TRUNCATE TABLE erp_cust_az12;
LOAD DATA LOCAL INFILE '/Users/stellavu/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

TRUNCATE TABLE erp_loc_a101;
LOAD DATA LOCAL INFILE '/Users/stellavu/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

TRUNCATE TABLE erp_px_cat_g1v2;
LOAD DATA LOCAL INFILE '/Users/stellavu/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

--Turn local_infile OFF
set global local_infile = 0;
