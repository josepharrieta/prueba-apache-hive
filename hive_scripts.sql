-- Crear base de datos
CREATE DATABASE IF NOT EXISTS ventas_db;
USE ventas_db;

-- Tabla externa (CSV en HDFS)
DROP TABLE IF EXISTS ventas_ext;
CREATE EXTERNAL TABLE ventas_ext (
  id_venta INT,
  fecha STRING,
  producto STRING,
  cantidad INT,
  precio FLOAT,
  cliente STRING,
  ciudad STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/ventas_ext';

-- Cargar datos en la tabla externa (desde el archivo CSV)
LOAD DATA LOCAL INPATH '/dataset_ventas.csv' OVERWRITE INTO TABLE ventas_ext;

-- Tabla interna
DROP TABLE IF EXISTS ventas_int;
CREATE TABLE ventas_int AS SELECT * FROM ventas_ext;

-- Consultas representativas
-- 1. Total de ventas por producto
SELECT producto, SUM(cantidad*precio) AS total_ventas FROM ventas_int GROUP BY producto;

-- 2. Ventas por ciudad
SELECT ciudad, COUNT(*) AS num_ventas FROM ventas_int GROUP BY ciudad;

-- 3. Producto más vendido
SELECT producto, SUM(cantidad) AS total_cantidad FROM ventas_int GROUP BY producto ORDER BY total_cantidad DESC LIMIT 1;

-- 4. Clientes únicos
SELECT COUNT(DISTINCT cliente) AS clientes_unicos FROM ventas_int;

-- 5. Ventas por día
SELECT fecha, SUM(cantidad*precio) AS total_dia FROM ventas_int GROUP BY fecha ORDER BY fecha;
