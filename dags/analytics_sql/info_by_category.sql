INSERT INTO `category_sales_analysis` (`category`, `total_revenue`, `total_quantity`, `load_date`)
SELECT 
    `p`.`category`, 
    SUM(`s`.`amount`) AS `total_revenue`, 
    SUM(`s`.`quantity`) AS `total_quantity`,
	'{{ ds }}' AS `load_date`
FROM `fact_sales` AS `s` 
INNER JOIN `dim_product` AS `p` ON `s`.`product_sk` = `p`.`product_sk` 
GROUP BY `p`.`category`, `s`.`sale_date`;

