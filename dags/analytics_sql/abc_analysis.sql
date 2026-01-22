INSERT INTO `abc_product_analysis` (`product_id`, `revenue`, `running_share`, `abc_category`, `load_date`)
WITH `products_revenue` AS (
    SELECT 
        `p`.`product_id`,
        SUM(`s`.`amount`) AS `revenue`
    FROM `fact_sales` AS `s` 
    INNER JOIN `dim_product` AS `p` ON `s`.`product_sk` = `p`.`product_sk`
    GROUP BY `p`.`product_id`
),
`calculated_shares` AS (
    SELECT
        `product_id`,
        `revenue`,
        SUM(`revenue`) OVER(ORDER BY `revenue` DESC) / SUM(`revenue`) OVER() AS `running_share`
    FROM `products_revenue`
)
SELECT 
    `product_id`,
    `revenue`,
    `running_share`,
    CASE
        WHEN `running_share` <= 0.80 THEN 'A'
        WHEN `running_share` <= 0.95 THEN 'B'
        ELSE 'C'
    END AS `abc_category`,
    '{{ ds }}' AS `load_date`
FROM `calculated_shares`;