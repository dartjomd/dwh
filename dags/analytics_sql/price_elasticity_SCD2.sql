REPLACE INTO `price_elasticity_analysis` (`name`, `price`, `is_current`, `total_sold`, `days_on_sale`, `sales_per_day`, `price_start`, `price_end`, `load_date`)
WITH `product_prices` AS (
    SELECT 
        `product_id`,
        `name`,
        `product_sk`,
        `price`,
        `is_current`,
        `start_date`,
        COALESCE(`end_date`, '{{ ds }}') AS `end_date`
    FROM `dim_product`
    WHERE `product_id` IN (
        SELECT `product_id` FROM `dim_product` GROUP BY `product_id` HAVING COUNT(*) > 1
    )
)
SELECT
    `p`.`name`,
    `p`.`price`,
    `is_current`,
    SUM(`s`.`quantity`) AS `total_sold`,
    GREATEST(DATEDIFF(`p`.`end_date`, `p`.`start_date`), 1) AS `days_on_sale`,
    SUM(`s`.`quantity`) / GREATEST(DATEDIFF(`p`.`end_date`, `p`.`start_date`), 1) AS `sales_per_day`,
    `p`.`start_date` AS `price_start`,
    `p`.`end_date` AS `price_end`,
    '{{ ds }}' AS `load_date`
FROM `product_prices` AS `p`
INNER JOIN `fact_sales` AS `s` ON `p`.`product_sk` = `s`.`product_sk`
GROUP BY `p`.`product_id`, `p`.`name`, `p`.`price`, `p`.`is_current`, `p`.`end_date`, `p`.`start_date`, `load_date`;