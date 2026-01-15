SELECT 
    `p`.`category`, 
    SUM(`s`.`amount`) AS `total_revenue`, 
    SUM(`s`.`quantity`) AS `total_quantity` 
FROM `fact_sales` AS `s` 
INNER JOIN `dim_product` AS `p` ON `s`.`product_sk` = `p`.`product_sk` 
GROUP BY `p`.`category` 
ORDER BY `total_revenue` DESC, `total_quantity` DESC;


with electronics_sales as (
	select c.city, s.amount  from fact_sales as s
		inner join dim_product as p
		on s.product_sk = p.product_sk
			and p.category = 'Electronics'
			and p.is_current = 1
		inner join dim_customer as c
		on s.customer_sk = c.customer_sk
)
select city, sum(amount) as revenue from electronics_sales
group by city
having revenue >= 10000
order by revenue desc




WITH products_revenue AS (
    SELECT 
        p.product_id,
        SUM(s.amount) AS revenue
    FROM fact_sales AS s 
    INNER JOIN dim_product AS p ON s.product_sk = p.product_sk
    GROUP BY p.product_id
),
calculated_shares AS (
	SELECT
		product_id,
		revenue,
		sum(revenue) over(order by revenue desc) / sum(revenue) over() AS running_share
	FROM products_revenue
)
SELECT 
	product_id,
	revenue,
	running_share,
	CASE
		WHEN running_share <= 0.80 THEN 'A'
		WHEN running_share <= 0.95 THEN 'B'
		ELSE 'C'
	END AS abc_category
FROM calculated_shares


with customer_purchases as (
	select
	c.customer_id,
	row_number() over(partition by c.customer_id order by s.sale_date asc) as purchase_number
	from fact_sales as s
	inner join dim_customer as c
	on s.customer_sk = c.customer_sk
),
raw_numbers as (
	select 
		count(distinct c.customer_id) as total_customers,
		count(distinct case when c.purchase_number = 2 then c.customer_id end) as returned
	from customer_purchases as c
)
select n.*, (n.returned * 100.0 / n.total_customers) as percentage from raw_numbers as n



WITH product_prices AS (
    SELECT 
        product_id,
        name,
        product_sk,
        price,
        is_current,
		start_date,
		COALESCE(end_date, CURRENT_DATE) AS end_date
    FROM dim_product
    WHERE product_id IN (
        SELECT product_id FROM dim_product GROUP BY product_id HAVING COUNT(*) > 1
    )
)
SELECT
	p.name,
	p.price,
	is_current,
	SUM(s.quantity) AS total_sold,
	GREATEST(DATEDIFF(p.end_date, p.start_date), 1) AS days_on_sale,
	SUM(s.quantity) / GREATEST(DATEDIFF(p.end_date, p.start_date), 1) AS sales_per_day
	
FROM product_prices AS p
INNER JOIN fact_sales AS s
ON p.product_sk = s.product_sk
GROUP BY p.product_id, p.name, p.price, p.is_current, p.end_date, p.start_date
ORDER BY p.product_id, p.name, p.price ASC
