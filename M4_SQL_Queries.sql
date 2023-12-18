-- TASK 1 --

SELECT country_code AS country,
  COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- TASK 2 --
SELECT locality, COUNT(*) as total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC;
-- TASK 3 --
SELECT
  SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
  dim_date_times.month
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY total_sales DESC;

-- TASK 4 --
SELECT COUNT(orders_table.product_quantity) AS numbers_of_sales, SUM(orders_table.product_quantity) AS product_quantity_count,
CASE
WHEN dim_store_details.store_code = 'Web' THEN 'Web'
ELSE 'Offline'
END AS location
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY location;

-- TASK 5 --
SELECT dim_store_details.store_type, 
SUM(orders_table.product_quantity*dim_products.product_price) as total_sales,
SUM(100.0*orders_table.product_quantity*dim_products.product_price)/(SUM(SUM(orders_table.product_quantity*dim_products.product_price)) over ()) AS percentage_total
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_store_details.store_type
ORDER BY total_sales DESC;

-- TASK 6 --
SELECT SUM(orders_table.product_quantity*dim_products.product_price) AS total_sales, dim_date_times.year, dim_date_times.month
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.year, dim_date_times.month
ORDER BY total_sales DESC;

-- TASK 7 --
SELECT SUM(dim_store_details.staff_numbers) AS total_staff_numbers, dim_store_details.country_code
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY dim_store_details.country_code
ORDER BY total_staff_numbers DESC;

-- TASK 8 --
SELECT SUM(orders_table.product_quantity*dim_products.product_price) AS total_sales, dim_store_details.store_type, dim_store_details.country_code
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE dim_store_details.country_code = 'DE' 
GROUP BY dim_store_details.store_type, dim_store_details.country_code
ORDER BY total_sales ASC;


-- TASK 9 --

SELECT dim_date_times.year, ROUND((SUM(orders_table.product_quantity)::decimal)/(365*24),2) AS sales_per_hour
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY dim_date_times.year
ORDER BY SUM(orders_table.product_quantity) DESC;


