-- Create table for abc analysis result
CREATE TABLE IF NOT EXISTS `abc_product_analysis` (
    `product_id` VARCHAR(20),
    `revenue` DECIMAL(10, 2) NOT NULL,
    `running_share` DECIMAL(5, 4) NOT NULL,
    `abc_category` VARCHAR(1) NOT NULL,
    `load_date` DATE NOT NULL,

    PRIMARY KEY (`product_id`, `load_date`)
);


--  Create view for abc analysis
CREATE OR REPLACE VIEW `v_abc_current_day` AS
SELECT *
FROM `abc_product_analysis`
WHERE `load_date` = (SELECT MAX(`load_date`) FROM `abc_product_analysis`)
ORDER BY `abc_category` ASC;

-- Create table for sales by category analysis result
CREATE TABLE IF NOT EXISTS `category_sales_analysis` (
    `category` VARCHAR(50),
    `total_revenue` DECIMAL(10, 2) NOT NULL,
    `total_quantity` INT NOT NULL,
    `load_date` DATE NOT NULL,

    PRIMARY KEY (`category`, `load_date`)
);

-- Create view for aggregation by category analysis
CREATE OR REPLACE VIEW `v_category_sales_analysis` AS 
SELECT *
FROM `category_sales_analysis`
WHERE `load_date` = (SELECT MAX(`load_date`) FROM `category_sales_analysis`);


-- Create table for price elasticity analysis
CREATE TABLE IF NOT EXISTS `price_elasticity_analysis` (
    `name` VARCHAR(255) NOT NULL,
    `price` DECIMAL(10, 2) NOT NULL,
    `is_current` BOOLEAN,
    `total_sold` INT NOT NULL,
    `days_on_sale` INT NOT NULL,
    `sales_per_day` DECIMAL(10, 2) NOT NULL,
    `price_start` DATE NOT NULL,
    `price_end` DATE NOT NULL,
    `load_date` DATE NOT NULL,

    PRIMARY KEY(`name`, `price`, `price_start`, `load_date`)
);

-- Create view for price elasticity analysis
CREATE OR REPLACE VIEW `v_price_elasticity_analysis` AS
SELECT *
FROM `price_elasticity_analysis`
WHERE `load_date` = (SELECT MAX(`load_date`) FROM `price_elasticity_analysis`)
ORDER BY `name`, `price` ASC;


-- Create table for customer retention analysis
CREATE TABLE IF NOT EXISTS `customer_retention_rate_analysis` (
    `load_date` DATE NOT NULL,
    `total_customers` INT NOT NULL,
    `returned` INT NOT NULL,
    `percentage` DECIMAL(10, 2) NOT NULL,

    PRIMARY KEY(`load_date`)
);

-- Create view for customer retention analysis
CREATE OR REPLACE VIEW `v_customer_retention_rate_analysis` AS
SELECT * FROM `customer_retention_rate_analysis`
WHERE `load_date` = (SELECT MAX(`load_date`) FROM `customer_retention_rate_analysis`);

-- Create table for revenue by city analysis
CREATE TABLE IF NOT EXISTS `revenue_by_city_analysis` (
    `city` VARCHAR(100) NOT NULL,
    `category` VARCHAR(100) NOT NULL,
    `revenue` DECIMAL(15, 2) NOT NULL,
    `load_date` DATE NOT NULL,

    PRIMARY KEY(`city`, `category`, `load_date`)
);

-- Create view for revenue by city
CREATE OR REPLACE VIEW `v_revenue_by_city_analysis` AS
SELECT * FROM `revenue_by_city_analysis`
WHERE `load_date` = (SELECT MAX(`load_date`) FROM `revenue_by_city_analysis`);
