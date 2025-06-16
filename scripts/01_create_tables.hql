-- =====================================================
-- Scripts HiveQL para Prueba de Concepto Apache Hive
-- Dataset: E-commerce Analytics
-- =====================================================

-- Crear base de datos para el proyecto
CREATE DATABASE IF NOT EXISTS ecommerce_analytics
COMMENT 'Base de datos para análisis de e-commerce'
LOCATION '/user/hive/warehouse/ecommerce_analytics.db';

USE ecommerce_analytics;

-- =====================================================
-- TABLA 1: CUSTOMERS (Tabla Interna)
-- =====================================================
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    country STRING,
    registration_date DATE,
    birth_date DATE,
    gender STRING,
    customer_segment STRING
)
COMMENT 'Tabla interna de clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- Cargar datos en tabla interna
LOAD DATA INPATH '/datasets/customers.csv' INTO TABLE customers;

-- =====================================================
-- TABLA 2: PRODUCTS (Tabla Externa)
-- =====================================================
DROP TABLE IF EXISTS products;

CREATE EXTERNAL TABLE products (
    product_id INT,
    product_name STRING,
    category STRING,
    brand STRING,
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    weight_kg DECIMAL(8,2),
    dimensions STRING,
    in_stock BOOLEAN,
    stock_quantity INT,
    supplier_id INT,
    launch_date DATE
)
COMMENT 'Tabla externa de productos'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/datasets/products/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- =====================================================
-- TABLA 3: ORDERS (Tabla Particionada)
-- =====================================================
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    order_status STRING,
    payment_method STRING,
    shipping_cost DECIMAL(8,2),
    tax_amount DECIMAL(8,2),
    discount_amount DECIMAL(8,2),
    total_amount DECIMAL(10,2),
    shipping_address STRING,
    delivery_date DATE,
    notes STRING
)
COMMENT 'Tabla de órdenes particionada por año'
PARTITIONED BY (year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- =====================================================
-- TABLA 4: ORDER_ITEMS (Tabla con Bucketing)
-- =====================================================
DROP TABLE IF EXISTS order_items;

CREATE TABLE order_items (
    item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_price DECIMAL(10,2),
    discount_applied DECIMAL(8,2)
)
COMMENT 'Tabla de items de órdenes con bucketing'
CLUSTERED BY (product_id) INTO 10 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- Cargar datos en tabla con bucketing
LOAD DATA INPATH '/datasets/order_items.csv' INTO TABLE order_items;

-- =====================================================
-- TABLA 5: REVIEWS (Tabla Externa con formato ORC)
-- =====================================================
DROP TABLE IF EXISTS reviews;

CREATE EXTERNAL TABLE reviews (
    review_id INT,
    customer_id INT,
    product_id INT,
    rating INT,
    review_text STRING,
    review_date DATE,
    helpful_votes INT,
    verified_purchase BOOLEAN
)
COMMENT 'Tabla externa de reseñas en formato ORC'
STORED AS ORC
LOCATION '/datasets/reviews_orc/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- Tabla temporal para carga de datos CSV
DROP TABLE IF EXISTS reviews_staging;

CREATE TABLE reviews_staging (
    review_id INT,
    customer_id INT,
    product_id INT,
    rating INT,
    review_text STRING,
    review_date DATE,
    helpful_votes INT,
    verified_purchase BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- Cargar datos en staging
LOAD DATA INPATH '/datasets/reviews.csv' INTO TABLE reviews_staging;

-- Insertar datos en tabla ORC optimizada
INSERT OVERWRITE TABLE reviews
SELECT * FROM reviews_staging;

-- =====================================================
-- VISTA: CUSTOMER_SUMMARY
-- =====================================================
DROP VIEW IF EXISTS customer_summary;

CREATE VIEW customer_summary AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.customer_segment,
    c.country,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.order_date) as last_order_date,
    DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) as days_since_last_order
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_segment, c.country;

-- =====================================================
-- Mostrar estructura de tablas creadas
-- =====================================================
SHOW TABLES;
DESCRIBE FORMATTED customers;
DESCRIBE FORMATTED products;
DESCRIBE FORMATTED orders;
DESCRIBE FORMATTED order_items;
DESCRIBE FORMATTED reviews;
