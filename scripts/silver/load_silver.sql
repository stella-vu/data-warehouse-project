/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		- Debugging the loading process
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/
DROP PROCEDURE IF EXISTS silver.load_silver;
DELIMITER $$

CREATE PROCEDURE silver.load_silver()
BEGIN
	-- Declare all time as datetime
    DECLARE batch_start_time DATETIME;
	DECLARE batch_end_time DATETIME;
	DECLARE start_time DATETIME;
	DECLARE end_time DATETIME;
	
    -- Error handling
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
        SELECT 'ERROR OCCURRED DURING LOADING SILVER LAYER' as msg;
    END;
   
   -- Temporary log table
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_log (
        msg VARCHAR(300),
        create_time DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    TRUNCATE TABLE tmp_log;
    
    -- Start loading silver layer
    SET batch_start_time = NOW();
    SET autocommit = 0;
    START TRANSACTION;
    
    INSERT INTO tmp_log (msg) VALUES 
	('=========================================='),
    ('Loading Silver Layer'),
    ('=========================================='),
    ('------------------------------------------'),
    ('Loading CRM tables'),
    ('------------------------------------------');
    
    -- Loading silver.crm_cust_info
    SET start_time = NOW();
    
    INSERT INTO tmp_log (msg) VALUES 
	('>> Truncating table silver.crm_cust_info');
    TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO tmp_log (msg) VALUES 
	('>> Inserting table silver.crm_cust_info');
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date 
	)
	SELECT          
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE
			WHEN UPPER(TRIM(cst_material_status)) = "M" THEN "Married"
			WHEN UPPER(TRIM(cst_material_status)) = "S" THEN "Single"
			ELSE "n/a"
		END cst_material_status, -- Normalize marital status values to readable format
		CASE
			WHEN UPPER(TRIM(cst_gndr)) = "M" THEN "Male"
			WHEN UPPER(TRIM(cst_gndr)) = "F" THEN "Female"
			ELSE "n/a"
		END cst_gndr, -- Normalize gender values to readable format
		cst_create_date
	FROM (
		SELECT 
			*,
			ROW_NUMBER() OVER (
				PARTITION BY cst_id 
				ORDER BY cst_create_date DESC
			) AS flag_last
		FROM bronze.crm_cust_info
	) t
	WHERE flag_last = 1 and cst_id != 0; -- Select the most recent record per customer
	
    SET end_time = NOW();
    INSERT INTO tmp_log (msg) VALUES 
    (CONCAT('Load duration: ',
			TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds'));
    
    -- =================================================
	-- Loading data to table silver.crm_prd_info
	SET start_time = NOW();
    
    INSERT INTO tmp_log (msg) VALUES 
    ('>> Truncating table silver.crm_prd_info');
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO tmp_log (msg) VALUES 
    ('>> Inserting data into: silver.crm_prd_info');
	INSERT INTO silver.crm_prd_info (
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
		REPLACE(SUBSTR(prd_key, 1, 5), "-","_") as cat_id, -- Extract category ID
		SUBSTRING(prd_key,7, LENGTH(prd_key)) as prd_key, -- Extract product key
		prd_nm,
		IFNULL(prd_cost, 0) as prd_cost,
		CASE 
			WHEN UPPER(TRIM(prd_line)) = "M" THEN "Mountain" 
			WHEN UPPER(TRIM(prd_line)) = "R" THEN "Road"
			WHEN UPPER(TRIM(prd_line)) = "S" THEN "Other Sales"
			WHEN UPPER(TRIM(prd_line)) = "T" THEN "Tool"
			ElSE "n/a"
		END prd_line, -- Map product line codes to descriptive values
		prd_start_dt,
		DATE_SUB(
			LEAD(prd_start_dt) 
				OVER (PARTITION BY prd_key ORDER BY prd_start_dt), 
			INTERVAL 1 DAY
		) AS prd_end_dt -- Calculate end date as one day before the next start date
	FROM bronze.crm_prd_info;
	
    SET end_time = NOW();
	INSERT INTO tmp_log (msg) VALUES 
    (CONCAT('Load duration: ',
			TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds'));

	-- =================================================
	-- Loading data to table silver.crm_sales_details
	SET start_time = NOW();
	INSERT INTO tmp_log (msg) VALUES 
    ('>> Trucating table silver.crm_sales_details');
    TRUNCATE TABLE silver.crm_sales_details;
	INSERT INTO tmp_log (msg) VALUES 
    ('>> Inserting data into: silver.crm_sales_details');
	INSERT INTO silver.crm_sales_details (
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
			WHEN sls_order_dt = 0 
				OR CHAR_LENGTH(CAST(sls_order_dt AS CHAR)) != 8 THEN NULL
			ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), "%Y%m%d")
		END sls_order_dt,
		STR_TO_DATE(CAST(sls_ship_dt AS CHAR), "%Y%m%d") AS sls_ship_dt,
		STR_TO_DATE(CAST(sls_due_dt AS CHAR), "%Y%m%d") AS sls_due_dt,
		CASE 
			WHEN sls_price = 0 THEN abs(sls_sales)
			WHEN sls_sales != sls_quantity * abs(sls_price)
				THEN sls_quantity * abs(sls_price)
			ELSE sls_sales
		END sls_sales, -- Recalculate sales if original value is missing or incorrect
		sls_quantity,
		CASE 
			WHEN sls_price <= 0 THEN sls_sales/sls_quantity
			ELSE sls_price
		END sls_price
	FROM bronze.crm_sales_details;
	
    SET end_time = NOW();
    INSERT INTO tmp_log (msg) VALUES (
		CONCAT('Load Duration: ', 
		TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds'));
   
	-- =================================================
    -- Loading ERP table
    INSERT INTO tmp_log (msg) VALUES 
    ('------------------------------------------'),
    ('Loading ERP tables'),
    ('------------------------------------------');
	-- =================================================
	-- Loading data to table silver.erp_cust_az12
	SET start_time = NOW();
    
    INSERT INTO tmp_log (msg) VALUES 
    ('>> Trucating table silver.erp_cust_az12');
    TRUNCATE TABLE silver.erp_cust_az12;
    
    INSERT INTO tmp_log (msg) VALUES 
	('>> Inserting data into: silver.erp_cust_az12');
	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) -- Remove 'NAS' prefix if present
			ELSE cid
		END AS cid,
		CASE
			WHEN 
				TIMESTAMPDIFF(YEAR, bdate, CURDATE())
				- (DATE_FORMAT(CURDATE(), '%m%d') < DATE_FORMAT(bdate, '%m%d'))
				BETWEEN 18 and 90
			THEN bdate 
			ELSE null -- Set birthday of customer > 90 years old or < 18 years old to NULL
		END bdate,
		CASE
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen -- Normalize gender values and handle unknown cases
	FROM bronze.erp_cust_az12;
	
    SET end_time = NOW();
    INSERT INTO tmp_log (msg) VALUES (
		CONCAT('Load Duration: ', 
		TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds'));
	
    -- =================================================
	-- Loading data to table silver.erp_loc_a101
	SET start_time = NOW();
    
	INSERT INTO tmp_log (msg) VALUES 
    ('>> Trucating table silver.crm_sales_details');
	TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO tmp_log (msg) VALUES 
    ('>> Inserting data into: silver.crm_sales_details');
	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
	)
	SELECT 
		REPLACE(cid, '-', '') as cid,
		CASE
			WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
			WHEN cntry = '' THEN 'n/a'
			WHEN cntry = 'DE' THEN 'Germany'
			ELSE cntry
		END cntry
	FROM bronze.erp_loc_a101;
    
    SET end_time = NOW();
    INSERT INTO tmp_log (msg) VALUES (
		CONCAT('Load Duration: ', 
		TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds'));

	-- =================================================
	-- Loading data to table silver.erp_px_cat_g1v2
	SET start_time = NOW();
    
    INSERT INTO tmp_log (msg) VALUES 
    ('>> Trucating table silver.crm_sales_details');
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO tmp_log (msg) VALUES 
	('>> Inserting data into: silver.crm_sales_details');
	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	
    SET end_time = NOW();
	INSERT INTO tmp_log (msg) VALUES
        (CONCAT('Load Duration: ', 
		TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds'))	
    
    COMMIT;
    
    SET batch_end_time = NOW();
    INSERT INTO tmp_log (msg) VALUES 
    ('=========================================='),
    ('Loading Silver layer completed'),
    (CONCAT('Total load duration: ', 
		TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds')),
	('==========================================');
    
    -- Return all logs in a single result set
    SELECT * FROM tmp_log;
    
    -- Clean up
    DROP TEMPORARY TABLE IF EXISTS tmp_log;
END$$
DELIMITER ;
