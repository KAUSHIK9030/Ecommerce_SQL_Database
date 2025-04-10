USE brazilian_ecommerce;

-- 1. Customers
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(5)
);

-- 3. Order Payments
CREATE TABLE order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value DECIMAL(10,2)
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_payments_dataset.csv'
INTO TABLE order_payments
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
-- 4. Orders
CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
select * from orders;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_orders_dataset.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    @order_id,
    @customer_id,
    @order_status,
    @order_purchase_timestamp,
    @order_approved_at,
    @order_delivered_carrier_date,
    @order_delivered_customer_date,
    @order_estimated_delivery_date
)
SET
    order_id = @order_id,
    customer_id = @customer_id,
    order_status = @order_status,
    order_purchase_timestamp = STR_TO_DATE(@order_purchase_timestamp, '%Y-%m-%d %H:%i:%s'),
    order_approved_at = CASE WHEN @order_approved_at = '' THEN NULL ELSE STR_TO_DATE(@order_approved_at, '%Y-%m-%d %H:%i:%s') END,
    order_delivered_carrier_date = CASE WHEN @order_delivered_carrier_date = '' THEN NULL ELSE STR_TO_DATE(@order_delivered_carrier_date, '%Y-%m-%d %H:%i:%s') END,
    order_delivered_customer_date = CASE WHEN @order_delivered_customer_date = '' THEN NULL ELSE STR_TO_DATE(@order_delivered_customer_date, '%Y-%m-%d %H:%i:%s') END,
    order_estimated_delivery_date = STR_TO_DATE(@order_estimated_delivery_date, '%Y-%m-%d %H:%i:%s');

-- customers
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_customers_dataset.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- 5. Sellers
CREATE TABLE sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(5)
);
select * from sellers;

-- 5. Sellers
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_sellers_dataset.csv'
INTO TABLE sellers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- 6. Product Category Name Translation (English)
CREATE TABLE product_category_translation (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);
-- 6. Product Category Translation
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/product_category_name_translation.csv'
INTO TABLE product_category_translation
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

---------------- SELECT,from,WHERE -----------------------------------------------------------------------------------------------------------------------------------------------------
#(Get all customers from state 'SP')
SELECT *
FROM customers
WHERE customer_state = 'SP';

----------------- select,from,groupby,orderby ------------------------------------------------------------------- 
#(Total number of orders per customer)
SELECT customer_id, COUNT(order_id) AS total_orders
FROM orders
GROUP BY customer_id
ORDER BY total_orders DESC;

-------------- JOIN --------------
#(Showing each customer's city and order status)
SELECT c.customer_id, c.customer_city, o.order_id, o.order_status
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
LIMIT 10;

-------------- LEFT JOIN --------------
#(All orders with or without corresponding payments)
SELECT o.order_id, o.order_status, op.payment_value
FROM orders o
LEFT JOIN order_payments op ON o.order_id = op.order_id
LIMIT 10;

-------------- SUBQUERY --------------
#(Customers with above-average total payment)
SELECT o.customer_id, SUM(op.payment_value) AS customer_total
FROM orders o
JOIN order_payments op ON o.order_id = op.order_id
GROUP BY o.customer_id
HAVING customer_total > (
    SELECT AVG(total_payment)
    FROM (
        SELECT o.customer_id, SUM(op.payment_value) AS total_payment
        FROM orders o
        JOIN order_payments op ON o.order_id = op.order_id
        GROUP BY o.customer_id
    ) AS avg_table
);

-------------- AGGREGATE FUNCTION --------------
#(Average payment per order)
SELECT AVG(payment_value) AS avg_payment_per_order
FROM order_payments;

----------- VIEW ---------
#(High-value customers (total payment > ₹1000))
CREATE VIEW high_value_customers AS
SELECT o.customer_id, SUM(op.payment_value) AS total_spent
FROM orders o
JOIN order_payments op ON o.order_id = op.order_id
GROUP BY o.customer_id
HAVING total_spent > 1000;

SELECT * FROM high_value_customers
ORDER BY total_spent DESC
LIMIT 10;

-------- INDEX -----------
#(customer_id lookups in orders)
CREATE INDEX idx_customer_id ON orders(customer_id);

SHOW INDEX FROM orders;