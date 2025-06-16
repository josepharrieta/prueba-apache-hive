# 🐘 Prueba de Concepto: Apache Hive

Esta es una prueba de concepto completa de Apache Hive usando Docker, que incluye un entorno funcional con datasets sintéticos de e-commerce y consultas HiveQL representativas.

## 📋 Contenido del Proyecto

- **Entorno completo Apache Hive** con Hadoop, PostgreSQL y Docker
- **Dataset sintético de e-commerce** con +96,000 registros
- **5 consultas HiveQL representativas** con funciones avanzadas
- **Tablas internas, externas, particionadas y con bucketing**
- **Scripts automatizados** para verificación y pruebas

## 🏗️ Arquitectura del Entorno

```
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   Hive Server   │  │  Hive Metastore │  │    PostgreSQL   │
│   (HiveQL)      │◄─┤   (Metadata)    │◄─┤   (Metastore)   │
│   Port: 10000   │  │   Port: 9083    │  │   Port: 5432    │
└─────────────────┘  └─────────────────┘  └─────────────────┘
         │                      │                      │
         ▼                      ▼                      ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   Hadoop HDFS   │  │   NameNode      │  │   DataNode      │
│   (Storage)     │◄─┤   Port: 9870    │──┤   Port: 9864    │
│                 │  │   Port: 9000    │  │                 │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

## 📊 Dataset Sintético

El proyecto genera automáticamente un dataset de e-commerce con las siguientes tablas:

| Tabla | Registros | Tipo | Descripción |
|-------|-----------|------|-------------|
| `customers` | 10,000 | Interna | Datos de clientes |
| `products` | 1,000 | Externa | Catálogo de productos |
| `orders` | 50,000 | Particionada | Órdenes de compra |
| `order_items` | ~125,000 | Bucketing | Items de órdenes |
| `reviews` | 25,000 | Externa ORC | Reseñas de productos |

## 🚀 Instrucciones de Instalación

### Prerrequisitos

- Docker Desktop instalado y corriendo
- Docker Compose disponible
- Al menos 8GB de RAM disponible
- PowerShell (Windows) o Bash (Linux/Mac)

### Paso 1: Clonar y Preparar

```powershell
# Navegar al directorio del proyecto
cd "c:\Users\josep\OneDrive\Documentos\GitHub\prueba-apache-hive"

# Verificar archivos
ls
```

### Paso 2: Generar Datasets

```powershell
# Instalar Python si no está instalado
# Instalar dependencias para generar datos
pip install faker

# Generar datasets sintéticos
python scripts/generate_datasets.py
```

### Paso 3: Iniciar Entorno Docker

```powershell
# Iniciar todos los servicios
docker-compose up -d

# Verificar que todos los contenedores estén corriendo
docker-compose ps
```

**⏱️ Tiempo estimado de inicio: 3-5 minutos**

### Paso 4: Preparar Datos en HDFS

```powershell
# Hacer el script ejecutable (en Linux/Mac)
# chmod +x scripts/prepare_data.sh

# Ejecutar preparación de datos
bash scripts/prepare_data.sh
```

### Paso 5: Crear Tablas Hive

```powershell
# Conectar a Hive y ejecutar scripts de creación
docker exec -i hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/01_create_tables.hql
```

### Paso 6: Verificar Instalación

```powershell
# Ejecutar script de verificación completa
bash scripts/verify_hive.sh
```

## 🔍 Verificación Paso a Paso

### 1. Verificar Contenedores

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

**Resultado esperado:**
```
NAMES                        STATUS          PORTS
hive-server                  Up X minutes    0.0.0.0:10000->10000/tcp, 0.0.0.0:10002->10002/tcp
hive-metastore              Up X minutes    0.0.0.0:9083->9083/tcp
namenode                    Up X minutes    0.0.0.0:9000->9000/tcp, 0.0.0.0:9870->9870/tcp
datanode                    Up X minutes    0.0.0.0:9864->9864/tcp
hive-metastore-postgresql   Up X minutes    0.0.0.0:5432->5432/tcp
```

### 2. Verificar Interfaces Web

- **HDFS NameNode:** http://localhost:9870
- **DataNode:** http://localhost:9864

### 3. Verificar Conectividad Hive

```powershell
# Conectar a Hive Server
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000

# Dentro de Beeline:
!connect jdbc:hive2://localhost:10000
# Usuario: cualquiera (presionar Enter)
# Password: (presionar Enter)

# Verificar bases de datos
SHOW DATABASES;

# Usar base de datos del proyecto
USE ecommerce_analytics;

# Mostrar tablas
SHOW TABLES;

# Salir
!quit
```

### 4. Verificar Datos en HDFS

```powershell
# Listar archivos en HDFS
docker exec namenode hdfs dfs -ls /datasets

# Verificar contenido de un archivo
docker exec namenode hdfs dfs -head /datasets/customers.csv
```

## 📝 Consultas HiveQL Representativas

### Consulta 1: Análisis de Ventas por Categoría

```sql
USE ecommerce_analytics;

SELECT 
    p.category,
    YEAR(o.order_date) as year,
    MONTH(o.order_date) as month,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(oi.total_price) as total_revenue,
    LAG(SUM(oi.total_price), 1) OVER (
        PARTITION BY p.category 
        ORDER BY YEAR(o.order_date), MONTH(o.order_date)
    ) as prev_month_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'completed'
GROUP BY p.category, YEAR(o.order_date), MONTH(o.order_date)
ORDER BY p.category, year DESC, month DESC;
```

### Consulta 2: Top 10 Clientes por Valor

```sql
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) as customer_name,
        c.customer_segment,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(o.total_amount) as total_spent,
        AVG(o.total_amount) as avg_order_value
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'completed'
    GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_segment
)
SELECT *,
    ROW_NUMBER() OVER (ORDER BY total_spent DESC) as value_rank
FROM customer_metrics
WHERE total_spent > (SELECT AVG(total_spent) FROM customer_metrics)
ORDER BY value_rank
LIMIT 10;
```

### Consulta 3: Análisis de Productos Performance

```sql
SELECT 
    p.product_name,
    p.category,
    p.brand,
    COUNT(DISTINCT oi.order_id) as times_ordered,
    SUM(oi.quantity) as total_units_sold,
    SUM(oi.total_price) as total_revenue,
    ROUND(AVG(r.rating), 2) as avg_rating,
    RANK() OVER (PARTITION BY p.category ORDER BY SUM(oi.total_price) DESC) as revenue_rank_in_category
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.order_id AND o.order_status = 'completed'
LEFT JOIN reviews r ON p.product_id = r.product_id
GROUP BY p.product_id, p.product_name, p.category, p.brand
HAVING COUNT(DISTINCT oi.order_id) >= 5
ORDER BY total_revenue DESC
LIMIT 20;
```

### Consulta 4: Análisis Estacional

```sql
SELECT 
    CASE 
        WHEN MONTH(o.order_date) IN (12, 1, 2) THEN 'Invierno'
        WHEN MONTH(o.order_date) IN (3, 4, 5) THEN 'Primavera'
        WHEN MONTH(o.order_date) IN (6, 7, 8) THEN 'Verano'
        WHEN MONTH(o.order_date) IN (9, 10, 11) THEN 'Otoño'
    END as season,
    p.category,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(oi.total_price) as total_revenue,
    PERCENTILE_APPROX(o.total_amount, 0.5) as median_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'completed'
GROUP BY 
    CASE 
        WHEN MONTH(o.order_date) IN (12, 1, 2) THEN 'Invierno'
        WHEN MONTH(o.order_date) IN (3, 4, 5) THEN 'Primavera'
        WHEN MONTH(o.order_date) IN (6, 7, 8) THEN 'Verano'
        WHEN MONTH(o.order_date) IN (9, 10, 11) THEN 'Otoño'
    END,
    p.category
ORDER BY season, total_revenue DESC;
```

### Consulta 5: Análisis de Cohortes

```sql
WITH customer_cohorts AS (
    SELECT 
        customer_id,
        MIN(order_date) as first_order_date,
        DATE_FORMAT(MIN(order_date), 'yyyy-MM') as cohort_month
    FROM orders
    WHERE order_status = 'completed'
    GROUP BY customer_id
),
cohort_data AS (
    SELECT 
        c.cohort_month,
        FLOOR(MONTHS_BETWEEN(o.order_date, c.first_order_date)) as period_number,
        COUNT(DISTINCT o.customer_id) as customers
    FROM orders o
    JOIN customer_cohorts c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'completed'
    GROUP BY c.cohort_month, FLOOR(MONTHS_BETWEEN(o.order_date, c.first_order_date))
)
SELECT 
    cohort_month,
    period_number,
    customers,
    LAG(customers) OVER (PARTITION BY cohort_month ORDER BY period_number) as prev_customers
FROM cohort_data
WHERE period_number <= 12
ORDER BY cohort_month, period_number;
```

## 🛠️ Funcionalidades Demostradas

### Tipos de Tablas

1. **Tabla Interna** (`customers`): Datos gestionados completamente por Hive
2. **Tabla Externa** (`products`): Referencias a datos externos
3. **Tabla Particionada** (`orders`): Particionada por año para optimización
4. **Tabla con Bucketing** (`order_items`): Distribuida en buckets por product_id
5. **Tabla ORC** (`reviews`): Formato columnar optimizado con compresión

### Funciones HiveQL Avanzadas

- **Window Functions:** LAG, ROW_NUMBER, RANK, PARTITION BY
- **Aggregate Functions:** SUM, COUNT, AVG, PERCENTILE_APPROX
- **Date Functions:** YEAR, MONTH, DATE_FORMAT, MONTHS_BETWEEN
- **String Functions:** CONCAT, CASE WHEN
- **Statistical Functions:** Percentiles, rankings
- **CTEs:** Common Table Expressions para consultas complejas

## 🔧 Troubleshooting

### Problema: Contenedores no inician

```powershell
# Verificar logs
docker-compose logs namenode
docker-compose logs hive-server

# Reiniciar servicios
docker-compose down
docker-compose up -d
```

### Problema: Error de conexión a Hive

```powershell
# Verificar que Hive Server esté listo
docker exec hive-server netstat -tlnp | grep 10000

# Esperar más tiempo para inicialización
sleep 60
```

### Problema: Tablas no se crean

```powershell
# Verificar que los datos estén en HDFS
docker exec namenode hdfs dfs -ls /datasets

# Verificar conexión a metastore
docker exec hive-metastore-postgresql psql -U hive -d metastore -c "\dt"
```

### Problema: Consultas lentas

```powershell
# Verificar configuración de memoria
docker stats

# Aumentar recursos de Docker Desktop si es necesario
```

## 📚 Archivos del Proyecto

```
prueba-apache-hive/
├── docker-compose.yml          # Configuración de servicios Docker
├── hadoop.env                  # Variables de entorno Hadoop/Hive
├── README.md                   # Este archivo
├── datasets/                   # Directorio para datasets generados
├── scripts/
│   ├── generate_datasets.py    # Generador de datos sintéticos
│   ├── 01_create_tables.hql    # Scripts de creación de tablas
│   ├── 02_queries.hql          # Consultas HiveQL representativas
│   ├── prepare_data.sh         # Preparación de datos en HDFS
│   └── verify_hive.sh          # Verificación completa del entorno
```

## 🎯 Casos de Uso Demostrados

1. **Analytics de E-commerce:** Análisis de ventas, clientes y productos
2. **Business Intelligence:** KPIs, métricas de rendimiento, segmentación
3. **Data Warehousing:** ETL, particionado, optimización de almacenamiento
4. **Big Data Processing:** Procesamiento de grandes volúmenes de datos
5. **Reporting:** Generación de reportes complejos con agregaciones

## 🏁 Conclusión

Esta prueba de concepto demuestra:

- ✅ **Configuración completa** de Apache Hive con Docker
- ✅ **Integración** con Hadoop HDFS y PostgreSQL
- ✅ **Variety de tipos de tablas** (internas, externas, particionadas, bucketing, ORC)
- ✅ **Consultas HiveQL avanzadas** con funciones complejas
- ✅ **Dataset realista** para casos de uso de analytics
- ✅ **Automatización** de deployment y verificación
- ✅ **Documentación completa** con troubleshooting

¡El entorno está listo para experimentar con Apache Hive y explorar sus capacidades de big data analytics! 🎉
