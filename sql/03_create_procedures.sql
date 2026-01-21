USE retail_dwh;

-- 
-- Procedure for SCD2 for clients
-- 
DROP PROCEDURE IF EXISTS `sp_load_dim_customers`;

DELIMITER $$

CREATE PROCEDURE `sp_load_dim_customers`()
BEGIN
    -- Create time batch of loading file into database
    DECLARE `min_ts` DATETIME;

    -- Disable safe mode
    SET SQL_SAFE_UPDATES = 0;

    -- Find minimal date in file to quarantee that start_data <= transaction_date
    SELECT MIN(`transaction_date`) INTO `min_ts` FROM `stg_raw_sales`;

    -- Exit if staging is empty
    IF `min_ts` IS NULL THEN
        SET SQL_SAFE_UPDATES = 1;
    ELSE
        -- Create temporary table for only clients whose data has been changed
        DROP TEMPORARY TABLE IF EXISTS `tmp_changed_customers`;
        CREATE TEMPORARY TABLE `tmp_changed_customers` (
            `customer_id` VARCHAR(100) PRIMARY KEY
        );

        -- Search these client records where data has been changed from latest dwh_dim_customer version 
        -- and insert IDs of those into temporary table
        INSERT INTO `tmp_changed_customers` (`customer_id`)
        SELECT DISTINCT `stg`.`customer_id`
        FROM `stg_raw_sales` AS `stg`
        INNER JOIN `dim_customer` AS `dwh` 
            ON `dwh`.`customer_id` = `stg`.`customer_id` 
            AND `dwh`.`is_current` = 1
        WHERE (`dwh`.`city` <> `stg`.`city` OR `dwh`.`email` <> `stg`.`email`);

        -- Close all old versions of customer (IS_CURRENT = 0)
        UPDATE `dim_customer` `dwh`
        INNER JOIN `tmp_changed_customers` AS `tmp` ON `dwh`.`customer_id` = `tmp`.`customer_id`
        SET 
            `dwh`.`end_date` = `min_ts`,
            `dwh`.`is_current` = 0
        WHERE `dwh`.`is_current` = 1;

        -- Insert updated customers
        INSERT INTO `dim_customer` (
            `customer_id`, `first_name`, `city`, `email`, `start_date`, `is_current`
        )
        SELECT 
            `stg_l`.`customer_id`,
            `stg_l`.`first_name`,
            `stg_l`.`city`,
            `stg_l`.`email`,
            `min_ts` AS `start_date`,
            1 AS `is_current`
        FROM (
            -- Select only latest version of customer
            SELECT 
                `customer_id`, `first_name`, `city`, `email`,
                ROW_NUMBER() OVER (PARTITION BY `customer_id` ORDER BY `transaction_date` DESC, `load_timestamp` DESC) as `rn`
            FROM `stg_raw_sales`
        ) AS `stg_l`
        LEFT JOIN `dim_customer` AS `dwh_c`
            ON `stg_l`.`customer_id` = `dwh_c`.`customer_id`
            -- Compare all fields to avoid inserting duplicate
            AND `stg_l`.`first_name` = `dwh_c`.`first_name`
            AND `stg_l`.`city` = `dwh_c`.`city`
            AND `stg_l`.`email` = `dwh_c`.`email`
            AND `dwh_c`.`is_current` = 1
        -- Insert if no matches were found
        WHERE `stg_l`.`rn` = 1 
          AND `dwh_c`.`customer_sk` IS NULL;

        DROP TEMPORARY TABLE IF EXISTS `tmp_changed_customers`;
        
        -- Return safe mode after transaction
        SET SQL_SAFE_UPDATES = 1;
    END IF;
END$$

DELIMITER ;


-- 
-- Procedure for SCD2 for products
-- 
DROP PROCEDURE IF EXISTS `sp_load_dim_products`;

DELIMITER $$

CREATE PROCEDURE `sp_load_dim_products`()
BEGIN
    -- Create time batch of loading file into database
    DECLARE `min_ts` DATETIME;
    
    -- Disable safe mode to execute query
    SET SQL_SAFE_UPDATES = 0;

    -- Find minimal date in file to quarantee that start_data <= transaction_date
    SELECT MIN(`transaction_date`) INTO `min_ts` FROM `stg_raw_sales`;

    -- Exit if staging is empty
    IF `min_ts` IS NULL THEN
        SET SQL_SAFE_UPDATES = 1;
    ELSE
        -- Create temporary table for only clients whose data has been changed
        DROP TEMPORARY TABLE IF EXISTS `tmp_changed_products`;
        CREATE TEMPORARY TABLE `tmp_changed_products` (
            `product_id` VARCHAR(100) PRIMARY KEY
        );

        -- Search these product records where data has been changed from latest dwh_dim_customer version
        -- and insert IDs of those into temporary table
        INSERT INTO `tmp_changed_products` (`product_id`)
        SELECT DISTINCT 
            `stg`.`product_id`
        FROM `stg_raw_sales` AS `stg`
        INNER JOIN `dim_product` AS `dwh`
            ON `dwh`.`product_id` = `stg`.`product_id`
            AND `dwh`.`is_current` = 1
        WHERE (`dwh`.`price` <> `stg`.`price` OR `dwh`.`name` <> `stg`.`product_name`);

        -- Set all changed products(changed price/name) to not active
        UPDATE `dim_product` AS `dwh`
        INNER JOIN `tmp_changed_products` AS `tmp`
        ON `dwh`.`product_id` = `tmp`.`product_id`
        SET 
            `dwh`.`end_date` = `min_ts`,
            `dwh`.`is_current` = 0
        WHERE `dwh`.`is_current` = 1;

        -- Insert updated products
        INSERT INTO `dim_product`
            (`product_id`, `name`, `price`, `category`, `start_date`, `is_current`)
        SELECT
            `stg_l`.`product_id`,
            `stg_l`.`product_name` AS `name`,
            `stg_l`.`price`,
            `stg_l`.`product_category` AS `category`,
            `min_ts` AS `start_date`,
            1 AS `is_current`
        FROM (
            -- Products data with extra field - rank number of date when item was purchased
            SELECT
                `product_id`,
                `product_name`,
                `price`,
                `product_category`,
                ROW_NUMBER() OVER(PARTITION BY `product_id` ORDER BY `transaction_date` DESC, `load_timestamp` DESC) AS `rn`
            FROM `stg_raw_sales`
        ) AS `stg_l`
        LEFT JOIN `dim_product` AS `dwh`
            ON `stg_l`.`product_id` = `dwh`.`product_id`
            -- Compare all fields to avoid inserting duplicate
            AND `stg_l`.`product_name` = `dwh`.`name`
            AND `stg_l`.`price` = `dwh`.`price`
            AND `stg_l`.`product_category` = `dwh`.`category`
            AND `dwh`.`is_current` = 1
        -- Insert if no matches were found
        WHERE `stg_l`.`rn` = 1
            AND `dwh`.`product_sk` IS NULL;

        DROP TEMPORARY TABLE IF EXISTS `tmp_changed_products`;

        -- Return safe mode after transaction
        SET SQL_SAFE_UPDATES = 1;
    END IF;
END$$

DELIMITER ;


-- 
-- Procedure for managing fact table
-- 
DROP PROCEDURE IF EXISTS `sp_load_fact_sales`;

DELIMITER $$

CREATE PROCEDURE `sp_load_fact_sales`()
BEGIN
    -- Disable safe mode to execute query
    SET SQL_SAFE_UPDATES = 0;

    INSERT INTO `fact_sales` (
        `transaction_id`, 
        `product_sk`, 
        `customer_sk`, 
        `sale_date`, 
        `amount`, 
        `quantity`, 
        `loaded_at`
    )
    SELECT
        `stg`.`transaction_id`,
        `dwh_p`.`product_sk`,
        `dwh_c`.`customer_sk`,
        `stg`.`transaction_date` AS `sale_date`,
        (`stg`.`price` * `stg`.`quantity`) AS `amount`,
        `stg`.`quantity`,
        NOW() AS `loaded_at`
    FROM 
        `stg_raw_sales` AS `stg`
    -- Retrieve product SK based on its active time range
    LEFT JOIN `dim_product` AS `dwh_p`
        ON `stg`.`product_id` = `dwh_p`.`product_id`
        AND `dwh_p`.`start_date` <= `stg`.`transaction_date`
        AND (`dwh_p`.`end_date` > `stg`.`transaction_date` OR `dwh_p`.`end_date` IS NULL)
    -- Retrieve customer SK based on his active time range
    LEFT JOIN `dim_customer` AS `dwh_c`
        ON `stg`.`customer_id` = `dwh_c`.`customer_id`
        AND `dwh_c`.`start_date` <= `stg`.`transaction_date`
        AND (`dwh_c`.`end_date` > `stg`.`transaction_date` OR `dwh_c`.`end_date` IS NULL)
    -- Check that transaction is not present in dwh_fact_sales table
    LEFT JOIN `fact_sales` AS `f`
        ON `f`.`transaction_id` = `stg`.`transaction_id`
    -- Insert if no matches were found
    WHERE `f`.`transaction_id` IS NULL;

    -- Return safe mode after transaction
    SET SQL_SAFE_UPDATES = 1;
END$$

DELIMITER ;