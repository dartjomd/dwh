USE retail_dwh;

CREATE TABLE IF NOT EXISTS `stg_raw_sales` (
    `raw_id` INT AUTO_INCREMENT PRIMARY KEY,
    `transaction_id` VARCHAR(50),
    `transaction_date` DATE,
    `customer_id` VARCHAR(50),
    `first_name` VARCHAR(100),
    `city` VARCHAR(100),
    `email` VARCHAR(100),
    `product_id` VARCHAR(50),
    `product_name` VARCHAR(255),
    `product_category` VARCHAR(100),
    `price` DECIMAL(10, 2),
    `quantity` INT,
    `load_timestamp` DATETIME
);

CREATE TABLE IF NOT EXISTS `failed_sales` (
    `raw_id` INT AUTO_INCREMENT PRIMARY KEY,
    `transaction_id` VARCHAR(100),
    `transaction_date` VARCHAR(100),
    `customer_id` VARCHAR(100),
    `first_name` VARCHAR(100),
    `city` VARCHAR(100),
    `email` VARCHAR(100),
    `product_id` VARCHAR(50),
    `product_name` VARCHAR(255),
    `product_category` VARCHAR(100),
    `price`VARCHAR(100),
    `quantity` VARCHAR(100),
    `load_timestamp` VARCHAR(100),
    `rejection_reason` VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS `dim_customer` (
    `customer_sk` INT AUTO_INCREMENT PRIMARY KEY,
    `customer_id` VARCHAR(50) NOT NULL,
    `first_name` VARCHAR(100) NOT NULL,
    `city` VARCHAR(100) NOT NULL,
    `email` VARCHAR(100) NOT NULL,
    `start_date` DATETIME NOT NULL,
    `end_date` DATETIME DEFAULT NULL,
    `is_current` BOOLEAN NOT NULL,
    INDEX `idx_customer_id` (`customer_id`)
);

CREATE TABLE IF NOT EXISTS `dim_product` (
    `product_sk` INT AUTO_INCREMENT PRIMARY KEY,
    `product_id` VARCHAR(50) NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `price` DECIMAL(10, 2) NOT NULL,
    `category` VARCHAR(100) NOT NULL,
    `start_date` DATETIME NOT NULL,
    `end_date` DATETIME DEFAULT NULL,
    `is_current` BOOLEAN NOT NULL,
    INDEX `idx_product_id` (`product_id`)
);

CREATE TABLE IF NOT EXISTS `fact_sales` (
    `sale_id` INT AUTO_INCREMENT PRIMARY KEY,
    `transaction_id` VARCHAR(50) NOT NULL,
    `customer_sk` INT NOT NULL,
    `product_sk` INT NOT NULL,
    `sale_date` DATE NOT NULL,
    `amount` DECIMAL(10, 2) NOT NULL,
    `quantity` INT NOT NULL,
    `loaded_at` DATETIME NOT NULL,
    FOREIGN KEY (`customer_sk`) REFERENCES `dim_customer`(`customer_sk`) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (`product_sk`) REFERENCES `dim_product`(`product_sk`) ON DELETE RESTRICT ON UPDATE CASCADE,
    INDEX `idx_transaction_id` (`transaction_id`)
);

CREATE TABLE IF NOT EXISTS `etl_stats` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `file_name` VARCHAR(255),
    `status` ENUM('success', 'failed'),
    `error_message` TEXT,
    `processed_at` DATETIME DEFAULT CURRENT_TIMESTAMP
);