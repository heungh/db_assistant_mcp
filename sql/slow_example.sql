-- Aurora MySQL 3 Slow Query Examples for E-commerce Schema
-- This file contains table creation, sample data, and examples of problematic queries

-- =============================================
-- Table Creation
-- =============================================

-- Customers Table
CREATE TABLE customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    registration_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_login DATETIME,
    total_orders INT DEFAULT 0,
    total_spent DECIMAL(12,2) DEFAULT 0.00,
    status ENUM('active', 'inactive', 'blocked') DEFAULT 'active'
);

-- Products Table
CREATE TABLE products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    category_id INT,
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    stock_quantity INT DEFAULT 0,
    weight DECIMAL(8,2),
    dimensions VARCHAR(50),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Categories Table
CREATE TABLE categories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    description TEXT,
    parent_category_id INT,
    level INT DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (parent_category_id) REFERENCES categories(category_id) ON DELETE SET NULL
);

-- Orders Table
CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded') DEFAULT 'pending',
    total_amount DECIMAL(12,2) NOT NULL,
    tax_amount DECIMAL(10,2) DEFAULT 0.00,
    shipping_amount DECIMAL(10,2) DEFAULT 0.00,
    discount_amount DECIMAL(10,2) DEFAULT 0.00,
    coupon_code VARCHAR(50),
    notes TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Order Items Table
CREATE TABLE order_items (
    order_item_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    subtotal DECIMAL(12,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0.00,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Payments Table
CREATE TABLE payments (
    payment_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    payment_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    payment_method ENUM('credit_card', 'debit_card', 'paypal', 'bank_transfer', 'cash_on_delivery') NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    transaction_id VARCHAR(100),
    status ENUM('pending', 'completed', 'failed', 'refunded') DEFAULT 'pending',
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Shipments Table
CREATE TABLE shipments (
    shipment_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    carrier VARCHAR(50) NOT NULL,
    tracking_number VARCHAR(100),
    shipping_date DATETIME,
    estimated_delivery DATETIME,
    actual_delivery DATETIME,
    status ENUM('processing', 'shipped', 'in_transit', 'delivered', 'failed') DEFAULT 'processing',
    shipping_address TEXT NOT NULL,
    shipping_city VARCHAR(50) NOT NULL,
    shipping_state VARCHAR(50),
    shipping_postal_code VARCHAR(20) NOT NULL,
    shipping_country VARCHAR(50) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Product Reviews Table
CREATE TABLE product_reviews (
    review_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_text TEXT,
    review_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    is_verified_purchase BOOLEAN DEFAULT FALSE,
    helpful_votes INT DEFAULT 0,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Inventory Table
CREATE TABLE inventory (
    inventory_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    warehouse_id INT NOT NULL,
    quantity INT NOT NULL DEFAULT 0,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (product_id, warehouse_id)
);

-- Warehouses Table
CREATE TABLE warehouses (
    warehouse_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    address VARCHAR(200) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50),
    postal_code VARCHAR(20) NOT NULL,
    country VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

-- Product Price History
CREATE TABLE product_price_history (
    history_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    old_price DECIMAL(10,2) NOT NULL,
    new_price DECIMAL(10,2) NOT NULL,
    change_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
-- =============================================
-- Sample Data
-- =============================================

-- Insert Categories
INSERT INTO categories (name, description, parent_category_id, level, is_active) VALUES
('Electronics', 'Electronic devices and accessories', NULL, 1, TRUE),
('Clothing', 'Apparel and fashion items', NULL, 1, TRUE),
('Home & Kitchen', 'Home goods and kitchen supplies', NULL, 1, TRUE),
('Smartphones', 'Mobile phones and accessories', 1, 2, TRUE),
('Laptops', 'Notebook computers', 1, 2, TRUE),
('Men''s Clothing', 'Clothing for men', 2, 2, TRUE),
('Women''s Clothing', 'Clothing for women', 2, 2, TRUE),
('Kitchen Appliances', 'Appliances for kitchen use', 3, 2, TRUE),
('Furniture', 'Home furniture items', 3, 2, TRUE),
('Android Phones', 'Android-based smartphones', 4, 3, TRUE),
('iPhones', 'Apple smartphones', 4, 3, TRUE),
('Gaming Laptops', 'Laptops designed for gaming', 5, 3, TRUE),
('Business Laptops', 'Laptops for professional use', 5, 3, TRUE),
('T-shirts', 'Casual t-shirts', 6, 3, TRUE),
('Jeans', 'Denim pants', 6, 3, TRUE),
('Dresses', 'Women''s dresses', 7, 3, TRUE),
('Skirts', 'Women''s skirts', 7, 3, TRUE),
('Refrigerators', 'Food cooling appliances', 8, 3, TRUE),
('Microwaves', 'Microwave ovens', 8, 3, TRUE),
('Sofas', 'Living room seating', 9, 3, TRUE);

-- Insert Warehouses
INSERT INTO warehouses (name, address, city, state, postal_code, country, is_active) VALUES
('Main Warehouse', '123 Storage Blvd', 'Seattle', 'WA', '98101', 'USA', TRUE),
('East Coast Facility', '456 Logistics Ave', 'New York', 'NY', '10001', 'USA', TRUE),
('West Coast Facility', '789 Shipping St', 'Los Angeles', 'CA', '90001', 'USA', TRUE),
('Central Distribution', '101 Warehouse Rd', 'Chicago', 'IL', '60601', 'USA', TRUE),
('Southern Hub', '202 Dispatch Ln', 'Atlanta', 'GA', '30301', 'USA', TRUE);

-- Insert Products (100 products)
INSERT INTO products (sku, name, description, category_id, price, cost, stock_quantity, weight, dimensions, is_active)
WITH RECURSIVE product_gen AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM product_gen WHERE n < 100
)
SELECT 
    CONCAT('SKU', LPAD(n, 5, '0')),
    CONCAT('Product ', n),
    CONCAT('Description for product ', n, '. This is a detailed description with multiple sentences to simulate real product data.'),
    (n % 20) + 1,
    50 + (n % 10) * 49.99,
    30 + (n % 10) * 25,
    100 + (n % 50) * 10,
    0.5 + (n % 10) * 0.3,
    CONCAT((n % 10) + 5, 'x', (n % 5) + 5, 'x', (n % 3) + 1),
    TRUE
FROM product_gen;

-- Insert Inventory
INSERT INTO inventory (product_id, warehouse_id, quantity)
WITH RECURSIVE inv_gen AS (
    SELECT 1 AS product_id, 1 AS warehouse_id
    UNION ALL
    SELECT 
        CASE 
            WHEN warehouse_id = 5 THEN product_id + 1
            ELSE product_id
        END,
        CASE 
            WHEN warehouse_id = 5 THEN 1
            ELSE warehouse_id + 1
        END
    FROM inv_gen 
    WHERE product_id <= 100
)
SELECT 
    product_id,
    warehouse_id,
    20 + (product_id % 50) * 5
FROM inv_gen
WHERE product_id <= 100;

-- Insert Customers (1000 customers)
INSERT INTO customers (first_name, last_name, email, phone, address, city, state, postal_code, country, registration_date, last_login, total_orders, total_spent, status)
WITH RECURSIVE cust_gen AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM cust_gen WHERE n < 1000
)
SELECT 
    CONCAT('FirstName', n),
    CONCAT('LastName', n),
    CONCAT('customer', n, '@example.com'),
    CONCAT('555-', LPAD(n % 10000, 4, '0')),
    CONCAT(n % 999 + 1, ' Main St'),
    CASE (n % 10)
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'Los Angeles'
        WHEN 2 THEN 'Chicago'
        WHEN 3 THEN 'Houston'
        WHEN 4 THEN 'Phoenix'
        WHEN 5 THEN 'Philadelphia'
        WHEN 6 THEN 'San Antonio'
        WHEN 7 THEN 'San Diego'
        WHEN 8 THEN 'Dallas'
        WHEN 9 THEN 'San Jose'
    END,
    CASE (n % 5)
        WHEN 0 THEN 'NY'
        WHEN 1 THEN 'CA'
        WHEN 2 THEN 'IL'
        WHEN 3 THEN 'TX'
        WHEN 4 THEN 'AZ'
    END,
    CONCAT(LPAD(n % 90000 + 10000, 5, '0')),
    'USA',
    DATE_SUB(NOW(), INTERVAL (n % 365) DAY),
    DATE_SUB(NOW(), INTERVAL (n % 30) DAY),
    (n % 20),
    (n % 20) * 100.00,
    CASE (n % 10)
        WHEN 0 THEN 'inactive'
        WHEN 9 THEN 'blocked'
        ELSE 'active'
    END
FROM cust_gen;
SET SESSION cte_max_recursion_depth = 50000;
-- Insert Orders (5000 orders)
INSERT INTO orders (customer_id, order_date, status, total_amount, tax_amount, shipping_amount, discount_amount, coupon_code)
WITH RECURSIVE order_gen AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM order_gen WHERE n < 5000
)
SELECT 
    (n % 1000) + 1,
    DATE_SUB(NOW(), INTERVAL (n % 365) DAY),
    CASE (n % 20)
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'processing'
        WHEN 2 THEN 'cancelled'
        WHEN 3 THEN 'refunded'
        WHEN 4 THEN 'processing'
        WHEN 19 THEN 'pending'
        ELSE 'delivered'
    END,
    100.00 + (n % 900),
    (100.00 + (n % 900)) * 0.08,
    10.00 + (n % 5) * 5,
    CASE WHEN n % 10 = 0 THEN 25.00 ELSE 0.00 END,
    CASE WHEN n % 10 = 0 THEN CONCAT('DISCOUNT', n % 100) ELSE NULL END
FROM order_gen;

-- Insert Order Items (15000 items)
INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal, discount_amount)
WITH RECURSIVE item_gen AS (
    SELECT 1 AS n, 1 AS order_id
    UNION ALL
    SELECT 
        n + 1,
        CASE WHEN n % 3 = 0 THEN order_id + 1 ELSE order_id END
    FROM item_gen 
    WHERE n < 15000 AND order_id <= 5000
)
SELECT 
    order_id,
    (n % 100) + 1,
    (n % 5) + 1,
    50.00 + (n % 10) * 10,
    (50.00 + (n % 10) * 10) * ((n % 5) + 1),
    CASE WHEN n % 20 = 0 THEN 15.00 ELSE 0.00 END
FROM item_gen;

-- Insert Payments
INSERT INTO payments (order_id, payment_date, payment_method, amount, transaction_id, status)
WITH RECURSIVE payment_gen AS (
    SELECT 1 AS order_id
    UNION ALL
    SELECT order_id + 1 FROM payment_gen WHERE order_id < 5000
)
SELECT 
    order_id,
    (SELECT order_date FROM orders WHERE order_id = payment_gen.order_id),
    CASE (order_id % 5)
        WHEN 0 THEN 'credit_card'
        WHEN 1 THEN 'debit_card'
        WHEN 2 THEN 'paypal'
        WHEN 3 THEN 'bank_transfer'
        WHEN 4 THEN 'cash_on_delivery'
    END,
    (SELECT total_amount FROM orders WHERE order_id = payment_gen.order_id),
    CONCAT('TXN', LPAD(order_id, 10, '0')),
    CASE 
        WHEN (SELECT status FROM orders WHERE order_id = payment_gen.order_id) IN ('cancelled', 'refunded') THEN 'refunded'
        WHEN (SELECT status FROM orders WHERE order_id = payment_gen.order_id) = 'pending' THEN 'pending'
        ELSE 'completed'
    END
FROM payment_gen;

-- Insert Shipments
INSERT INTO shipments (order_id, carrier, tracking_number, shipping_date, estimated_delivery, actual_delivery, status, 
                      shipping_address, shipping_city, shipping_state, shipping_postal_code, shipping_country)
WITH RECURSIVE shipment_gen AS (
    SELECT 1 AS order_id
    UNION ALL
    SELECT order_id + 1 FROM shipment_gen WHERE order_id < 5000
)
SELECT 
    order_id,
    CASE (order_id % 4)
        WHEN 0 THEN 'FedEx'
        WHEN 1 THEN 'UPS'
        WHEN 2 THEN 'USPS'
        WHEN 3 THEN 'DHL'
    END,
    CONCAT('TRK', LPAD(order_id, 10, '0')),
    CASE 
        WHEN (SELECT status FROM orders WHERE order_id = shipment_gen.order_id) IN ('pending', 'cancelled') THEN NULL
        ELSE DATE_ADD((SELECT order_date FROM orders WHERE order_id = shipment_gen.order_id), INTERVAL 1 DAY)
    END,
    CASE 
        WHEN (SELECT status FROM orders WHERE order_id = shipment_gen.order_id) IN ('pending', 'cancelled') THEN NULL
        ELSE DATE_ADD((SELECT order_date FROM orders WHERE order_id = shipment_gen.order_id), INTERVAL 5 DAY)
    END,
    CASE 
        WHEN (SELECT status FROM orders WHERE order_id = shipment_gen.order_id) = 'delivered' THEN 
            DATE_ADD((SELECT order_date FROM orders WHERE order_id = shipment_gen.order_id), INTERVAL 4 DAY)
        ELSE NULL
    END,
    CASE 
        WHEN (SELECT status FROM orders WHERE order_id = shipment_gen.order_id) = 'pending' THEN 'processing'
        WHEN (SELECT status FROM orders WHERE order_id = shipment_gen.order_id) = 'processing' THEN 'processing'
        WHEN (SELECT status FROM orders WHERE order_id = shipment_gen.order_id) = 'shipped' THEN 'in_transit'
        WHEN (SELECT status FROM orders WHERE order_id = shipment_gen.order_id) = 'delivered' THEN 'delivered'
        ELSE 'processing'
    END,
    CONCAT((order_id % 999) + 1, ' Delivery St'),
    CASE (order_id % 10)
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'Los Angeles'
        WHEN 2 THEN 'Chicago'
        WHEN 3 THEN 'Houston'
        WHEN 4 THEN 'Phoenix'
        WHEN 5 THEN 'Philadelphia'
        WHEN 6 THEN 'San Antonio'
        WHEN 7 THEN 'San Diego'
        WHEN 8 THEN 'Dallas'
        WHEN 9 THEN 'San Jose'
    END,
    CASE (order_id % 5)
        WHEN 0 THEN 'NY'
        WHEN 1 THEN 'CA'
        WHEN 2 THEN 'IL'
        WHEN 3 THEN 'TX'
        WHEN 4 THEN 'AZ'
    END,
    CONCAT(LPAD((order_id % 90000) + 10000, 5, '0')),
    'USA'
FROM shipment_gen
WHERE (SELECT status FROM orders WHERE order_id = shipment_gen.order_id) NOT IN ('cancelled', 'refunded');

-- Insert Product Reviews (2000 reviews)
INSERT INTO product_reviews (product_id, customer_id, rating, review_text, review_date, is_verified_purchase, helpful_votes)
WITH RECURSIVE review_gen AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM review_gen WHERE n < 2000
)
SELECT 
    (n % 100) + 1,
    (n % 1000) + 1,
    (n % 5) + 1,
    CASE (n % 5) + 1
        WHEN 1 THEN 'Very disappointed with this product. Would not recommend.'
        WHEN 2 THEN 'Below average product. Has some issues.'
        WHEN 3 THEN 'Average product. Does what it says.'
        WHEN 4 THEN 'Good product. Happy with my purchase.'
        WHEN 5 THEN 'Excellent product! Exceeded my expectations. Highly recommended!'
    END,
    DATE_SUB(NOW(), INTERVAL (n % 180) DAY),
    CASE WHEN n % 3 = 0 THEN TRUE ELSE FALSE END,
    n % 50
FROM review_gen;

-- Insert Product Price History (300 entries)
INSERT INTO product_price_history (product_id, old_price, new_price, change_date)
WITH RECURSIVE price_gen AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM price_gen WHERE n < 300
)
SELECT 
    (n % 100) + 1,
    50.00 + (n % 10) * 10,
    55.00 + (n % 10) * 10,
    DATE_SUB(NOW(), INTERVAL (n % 365) DAY)
FROM price_gen;

-- Add some indexes (but not all needed ones, to demonstrate slow queries)
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_payments_order_id ON payments(order_id);
CREATE INDEX idx_shipments_order_id ON shipments(order_id);
-- =============================================
-- Problematic Queries
-- =============================================

-- Query 1: Missing Index on Frequently Filtered Column
-- Problem: No index on product_reviews.rating despite frequent filtering
-- This query finds all 5-star reviews with high helpful votes, but will perform a full table scan
EXPLAIN
SELECT pr.review_id, pr.product_id, p.name AS product_name, pr.customer_id, 
       c.first_name, c.last_name, pr.rating, pr.review_text, pr.helpful_votes
FROM product_reviews pr
JOIN products p ON pr.product_id = p.product_id
JOIN customers c ON pr.customer_id = c.customer_id
WHERE pr.rating = 5
AND pr.helpful_votes > 20
ORDER BY pr.helpful_votes DESC
LIMIT 100;

-- Query 2: Inefficient LIKE with Leading Wildcard
-- Problem: Using LIKE with leading wildcard prevents index usage
-- This query searches for products with names ending in specific text
EXPLAIN
SELECT p.product_id, p.name, p.price, p.stock_quantity, c.name AS category
FROM products p
JOIN categories c ON p.category_id = c.category_id
WHERE p.name LIKE '%Phone%'
AND p.is_active = TRUE
ORDER BY p.price DESC;

-- Query 3: Correlated Subquery Instead of JOIN
-- Problem: Inefficient correlated subquery that runs for each row
-- This query finds customers with high-value orders using a correlated subquery
EXPLAIN
SELECT c.customer_id, c.first_name, c.last_name, c.email,
       (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) AS order_count,
       (SELECT SUM(total_amount) FROM orders o WHERE o.customer_id = c.customer_id) AS total_spent
FROM customers c
WHERE (SELECT MAX(total_amount) FROM orders o WHERE o.customer_id = c.customer_id) > 500
ORDER BY total_spent DESC;

-- Query 4: Inefficient OR Conditions Preventing Index Usage
-- Problem: OR conditions often prevent optimal index usage
-- This query searches for orders with various status conditions
EXPLAIN
SELECT o.order_id, o.customer_id, o.order_date, o.status, o.total_amount,
       c.first_name, c.last_name, c.email
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE (o.status = 'pending' OR o.status = 'processing')
AND (o.total_amount > 200 OR o.discount_amount > 50)
ORDER BY o.order_date DESC
LIMIT 100;

-- Query 5: Cartesian Product (Missing JOIN Condition)
-- Problem: Missing JOIN condition creates a cartesian product
-- This query attempts to find products and their warehouses but misses a JOIN condition
EXPLAIN
SELECT p.product_id, p.name, p.stock_quantity, w.warehouse_id, w.name AS warehouse_name
FROM products p, warehouses w, inventory i
WHERE p.stock_quantity < 50
AND i.quantity < 10
ORDER BY p.stock_quantity ASC;

-- Query 6: Inefficient GROUP BY Without Proper Indexes
-- Problem: GROUP BY on columns without proper indexes
-- This query analyzes sales by category and city but lacks proper indexes
EXPLAIN
SELECT c.category_id, c.name AS category_name, cu.city,
       COUNT(o.order_id) AS order_count,
       SUM(oi.quantity) AS items_sold,
       SUM(oi.subtotal) AS total_sales
FROM orders o
JOIN customers cu ON o.customer_id = cu.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
WHERE o.order_date BETWEEN DATE_SUB(NOW(), INTERVAL 90 DAY) AND NOW()
GROUP BY c.category_id, c.name, cu.city
ORDER BY total_sales DESC;

-- Query 7: Inefficient Sorting Without Index
-- Problem: Sorting on a column without an index
-- This query sorts products by price but there's no index on the price column
EXPLAIN
SELECT p.product_id, p.name, p.price, p.stock_quantity, c.name AS category
FROM products p
JOIN categories c ON p.category_id = c.category_id
WHERE p.is_active = TRUE
ORDER BY p.price ASC
LIMIT 1000;

-- Query 8: Complex JOIN with Poor Join Order
-- Problem: Inefficient join order causing large intermediate results
-- This query analyzes customer purchasing patterns with a poor join order
EXPLAIN
SELECT c.customer_id, c.first_name, c.last_name, 
       p.product_id, p.name AS product_name,
       COUNT(oi.order_item_id) AS purchase_count,
       SUM(oi.quantity) AS total_quantity
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE c.registration_date > DATE_SUB(NOW(), INTERVAL 1 YEAR)
AND p.category_id IN (4, 5, 10, 11) -- Electronics categories
GROUP BY c.customer_id, c.first_name, c.last_name, p.product_id, p.name
HAVING COUNT(oi.order_item_id) > 1
ORDER BY purchase_count DESC;

-- Query 9: Inefficient Subqueries in SELECT Clause
-- Problem: Multiple subqueries in SELECT clause causing repeated execution
-- This query gets product details with subqueries for inventory and reviews
EXPLAIN
SELECT p.product_id, p.name, p.price,
       (SELECT SUM(quantity) FROM inventory i WHERE i.product_id = p.product_id) AS total_stock,
       (SELECT COUNT(*) FROM product_reviews pr WHERE pr.product_id = p.product_id) AS review_count,
       (SELECT AVG(rating) FROM product_reviews pr WHERE pr.product_id = p.product_id) AS avg_rating,
       (SELECT COUNT(*) FROM order_items oi WHERE oi.product_id = p.product_id) AS times_ordered
FROM products p
WHERE p.category_id IN (
    SELECT category_id FROM categories WHERE parent_category_id = 1 -- Electronics subcategories
)
ORDER BY avg_rating DESC, times_ordered DESC
LIMIT 50;

-- Query 10: Inefficient Date Range Query Without Proper Index
-- Problem: Date range query without an index on the date column
-- This query analyzes daily sales over a period but lacks a date index
EXPLAIN
SELECT DATE(o.order_date) AS sale_date,
       COUNT(o.order_id) AS orders,
       SUM(o.total_amount) AS total_sales,
       AVG(o.total_amount) AS avg_order_value
FROM orders o
WHERE o.order_date BETWEEN DATE_SUB(NOW(), INTERVAL 6 MONTH) AND NOW()
AND o.status NOT IN ('cancelled', 'refunded')
GROUP BY DATE(o.order_date)
ORDER BY sale_date DESC;

-- Query 11: Inefficient Self-Join Without Proper Indexes
-- Problem: Self-join on categories table without proper indexing
-- This query finds all category hierarchies but performs poorly
EXPLAIN
SELECT c1.category_id, c1.name, 
       c2.category_id AS parent_id, c2.name AS parent_name,
       c3.category_id AS grandparent_id, c3.name AS grandparent_name
FROM categories c1
LEFT JOIN categories c2 ON c1.parent_category_id = c2.category_id
LEFT JOIN categories c3 ON c2.parent_category_id = c3.category_id
WHERE c1.level = 3
ORDER BY c3.name, c2.name, c1.name;

-- Query 12: Function on Indexed Column Preventing Index Usage
-- Problem: Using functions on indexed columns prevents index usage
-- This query searches for orders by month but the function prevents index usage
EXPLAIN
SELECT MONTH(o.order_date) AS month, 
       YEAR(o.order_date) AS year,
       COUNT(*) AS order_count,
       SUM(o.total_amount) AS total_sales
FROM orders o
WHERE YEAR(o.order_date) = YEAR(CURRENT_DATE - INTERVAL 1 YEAR)
GROUP BY YEAR(o.order_date), MONTH(o.order_date)
ORDER BY year, month;

-- Query 13: Complex Query with Multiple JOINs and Aggregations
-- Problem: Complex query with multiple joins and no covering indexes
-- This query analyzes customer purchasing patterns across categories and time
EXPLAIN
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    cat.name AS category_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(oi.quantity) AS total_items,
    SUM(oi.subtotal) AS total_spent,
    AVG(pr.rating) AS avg_rating
FROM 
    customers c
JOIN 
    orders o ON c.customer_id = o.customer_id
JOIN 
    order_items oi ON o.order_id = oi.order_id
JOIN 
    products p ON oi.product_id = p.product_id
JOIN 
    categories cat ON p.category_id = cat.category_id
LEFT JOIN 
    product_reviews pr ON p.product_id = pr.product_id AND pr.customer_id = c.customer_id
WHERE 
    o.order_date > DATE_SUB(NOW(), INTERVAL 1 YEAR)
    AND o.status = 'delivered'
GROUP BY 
    c.customer_id, customer_name, cat.name
HAVING 
    COUNT(DISTINCT o.order_id) > 2
ORDER BY 
    total_spent DESC
LIMIT 100;

-- Query 14: Inefficient IN Subquery
-- Problem: Using IN with a complex subquery instead of JOIN
-- This query finds products that have been ordered more than 10 times
EXPLAIN
SELECT p.product_id, p.name, p.price, p.stock_quantity
FROM products p
WHERE p.product_id IN (
    SELECT oi.product_id
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_date > DATE_SUB(NOW(), INTERVAL 30 DAY)
    GROUP BY oi.product_id
    HAVING COUNT(DISTINCT o.order_id) > 10
)
ORDER BY p.stock_quantity ASC;

-- Query 15: Inefficient UNION with Complex Subqueries
-- Problem: Using UNION with complex subqueries instead of more efficient alternatives
-- This query finds both top-selling and top-rated products
EXPLAIN
(SELECT p.product_id, p.name, p.price, 'Top Selling' AS category,
        SUM(oi.quantity) AS metric
 FROM products p
 JOIN order_items oi ON p.product_id = oi.product_id
 JOIN orders o ON oi.order_id = o.order_id
 WHERE o.order_date > DATE_SUB(NOW(), INTERVAL 90 DAY)
 GROUP BY p.product_id, p.name, p.price
 ORDER BY metric DESC
 LIMIT 10)
UNION
(SELECT p.product_id, p.name, p.price, 'Top Rated' AS category,
        AVG(pr.rating) AS metric
 FROM products p
 JOIN product_reviews pr ON p.product_id = pr.product_id
 GROUP BY p.product_id, p.name, p.price
 HAVING COUNT(pr.review_id) > 5
 ORDER BY metric DESC
 LIMIT 10)
ORDER BY category, metric DESC;

-- =============================================
-- Tuning Recommendations
-- =============================================

/*
Query 1: Add index on rating and helpful_votes
CREATE INDEX idx_product_reviews_rating_votes ON product_reviews(rating, helpful_votes);

Query 2: Consider full-text search instead of LIKE with leading wildcard
CREATE FULLTEXT INDEX idx_products_name ON products(name);
Then use: WHERE MATCH(p.name) AGAINST('Phone' IN BOOLEAN MODE)

Query 3: Replace with JOIN and GROUP BY
SELECT c.customer_id, c.first_name, c.last_name, c.email,
       COUNT(o.order_id) AS order_count,
       SUM(o.total_amount) AS total_spent
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
HAVING MAX(o.total_amount) > 500
ORDER BY total_spent DESC;

Query 4: Rewrite using UNION or add composite index
CREATE INDEX idx_orders_status_amount ON orders(status, total_amount, discount_amount);

Query 5: Fix JOIN conditions
SELECT p.product_id, p.name, p.stock_quantity, w.warehouse_id, w.name AS warehouse_name
FROM products p
JOIN inventory i ON p.product_id = i.product_id
JOIN warehouses w ON i.warehouse_id = w.warehouse_id
WHERE p.stock_quantity < 50
AND i.quantity < 10
ORDER BY p.stock_quantity ASC;

Query 6: Add composite indexes
CREATE INDEX idx_customers_city ON customers(city);
CREATE INDEX idx_orders_date ON orders(order_date);

Query 7: Add index on price
CREATE INDEX idx_products_price ON products(price);

Query 8: Optimize join order and add indexes
CREATE INDEX idx_customers_reg_date ON customers(registration_date);
CREATE INDEX idx_products_category_id ON products(category_id);

Query 9: Replace subqueries with JOINs
SELECT p.product_id, p.name, p.price,
       SUM(i.quantity) AS total_stock,
       COUNT(DISTINCT pr.review_id) AS review_count,
       AVG(pr.rating) AS avg_rating,
       COUNT(DISTINCT oi.order_item_id) AS times_ordered
FROM products p
LEFT JOIN inventory i ON p.product_id = i.product_id
LEFT JOIN product_reviews pr ON p.product_id = pr.product_id
LEFT JOIN order_items oi ON p.product_id = oi.product_id
WHERE p.category_id IN (
    SELECT category_id FROM categories WHERE parent_category_id = 1
)
GROUP BY p.product_id, p.name, p.price
ORDER BY avg_rating DESC, times_ordered DESC
LIMIT 50;

Query 10: Add index on order_date
CREATE INDEX idx_orders_date_status ON orders(order_date, status);

Query 11: Add index on parent_category_id and level
CREATE INDEX idx_categories_parent_level ON categories(parent_category_id, level);

Query 12: Rewrite query to avoid functions on indexed columns
CREATE INDEX idx_orders_order_date ON orders(order_date);
SELECT MONTH(o.order_date) AS month, 
       YEAR(o.order_date) AS year,
       COUNT(*) AS order_count,
       SUM(o.total_amount) AS total_sales
FROM orders o
WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY YEAR(o.order_date), MONTH(o.order_date)
ORDER BY year, month;

Query 13: Add covering indexes and optimize join order
CREATE INDEX idx_orders_customer_date_status ON orders(customer_id, order_date, status);
CREATE INDEX idx_order_items_order_product ON order_items(order_id, product_id, quantity, subtotal);
CREATE INDEX idx_products_category ON products(category_id, product_id);

Query 14: Replace IN subquery with JOIN
SELECT p.product_id, p.name, p.price, p.stock_quantity
FROM products p
JOIN (
    SELECT oi.product_id
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_date > DATE_SUB(NOW(), INTERVAL 30 DAY)
    GROUP BY oi.product_id
    HAVING COUNT(DISTINCT o.order_id) > 10
) AS frequent_products ON p.product_id = frequent_products.product_id
ORDER BY p.stock_quantity ASC;

Query 15: Replace UNION with separate queries or optimize with proper indexes
CREATE INDEX idx_order_items_product_quantity ON order_items(product_id, quantity);
CREATE INDEX idx_product_reviews_product_rating ON product_reviews(product_id, rating);
*/
