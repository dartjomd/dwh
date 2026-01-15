WITH `electronics_sales` AS (
    SELECT 
        `c`.`city`, 
        `s`.`amount`  
    FROM `fact_sales` AS `s`
    INNER JOIN `dim_product` AS `p` ON `s`.`product_sk` = `p`.`product_sk` 
        AND `p`.`category` = 'Electronics'
        AND `p`.`is_current` = 1
    INNER JOIN `dim_customer` AS `c` ON `s`.`customer_sk` = `c`.`customer_sk`
)
SELECT 
    `city`, 
    SUM(`amount`) AS `revenue` 
FROM `electronics_sales`
GROUP BY `city`
HAVING `revenue` >= 10000
ORDER BY `revenue` DESC;