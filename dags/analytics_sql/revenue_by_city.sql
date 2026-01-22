INSERT INTO `revenue_by_city_analysis` (`city`, `category`, `revenue`, `load_date`)
WITH `sales` AS (
    SELECT 
        `c`.`city`, 
        `s`.`amount`,
        `p`.`category`
    FROM `fact_sales` AS `s`
    INNER JOIN `dim_product` AS `p` ON `s`.`product_sk` = `p`.`product_sk` 
    INNER JOIN `dim_customer` AS `c` ON `s`.`customer_sk` = `c`.`customer_sk`
)
SELECT 
    `city`,
    `category`,
    SUM(`amount`) AS `revenue`,
    '{{ ds }}' AS `load_date`
FROM `sales`
GROUP BY `city`, `category`, `load_date`
ORDER BY `revenue` DESC;