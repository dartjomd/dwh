REPLACE INTO `customer_retention_rate_analysis` (`total_customers`, `returned`, `percentage`, `load_date`)
WITH `customer_purchases` AS (
    SELECT
        `c`.`customer_id`,
        ROW_NUMBER() OVER(PARTITION BY `c`.`customer_id` ORDER BY `s`.`sale_date` ASC) AS `purchase_number`
    FROM `fact_sales` AS `s`
    INNER JOIN `dim_customer` AS `c` ON `s`.`customer_sk` = `c`.`customer_sk`
),
`raw_numbers` AS (
    SELECT 
        COUNT(DISTINCT `c`.`customer_id`) AS `total_customers`,
        COUNT(DISTINCT CASE WHEN `c`.`purchase_number` >= 2 THEN `c`.`customer_id` END) AS `returned`
    FROM `customer_purchases` AS `c`
)
SELECT 
    `n`.*, 
    ROUND((`n`.`returned` * 100.0 / `n`.`total_customers`)) AS `percentage`,
    '{{ ds }}' AS `load_date`
FROM `raw_numbers` AS `n`;