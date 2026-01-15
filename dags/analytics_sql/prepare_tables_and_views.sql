-- Create table for abc analysis result
CREATE TABLE IF NOT EXISTS `abc_product_analysis` (
    `product_id` VARCHAR(20),
    `revenue` DECIMAL(10, 2) NOT NULL,
    `running_share` DECIMAL(5, 4) NOT NULL,
    `abc_category` VARCHAR(1) NOT NULL,
    `date` DATE NOT NULL,

    PRIMARY KEY (`product_id`, `date`)
);


--  Create abc analysis view for metabase
CREATE OR REPLACE VIEW `v_abc_current_day` AS
SELECT *
FROM `abc_product_analysis`
WHERE `date` = (SELECT MAX(`date`) FROM `abc_product_analysis`);

-- Create table for abc analysis result
CREATE TABLE IF NOT EXISTS `category_sales_analysis` (
    `category` VARCHAR(50),
    `total_revenue` DECIMAL(10, 2) NOT NULL,
    `total_quantity` INT NOT NULL,
    `date` DATE NOT NULL,

    PRIMARY KEY (`category`, `date`)
);

-- Create aggregation by category analysis view for metabase
CREATE OR REPLACE VIEW `v_category_sales_analysis` AS 
SELECT *
FROM `category_sales_analysis`
WHERE `date` = (SELECT MAX(`date`) FROM `category_sales_analysis`)