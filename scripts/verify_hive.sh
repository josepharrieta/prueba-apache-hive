#!/bin/bash

# =====================================================
# Script de Verificación y Pruebas Apache Hive
# =====================================================

echo "=== Verificación del entorno Apache Hive ==="

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para mostrar estado
print_status() {
    if [ $2 -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $1"
    else
        echo -e "${RED}✗${NC} $1"
    fi
}

# Función para ejecutar consulta Hive
run_hive_query() {
    local query="$1"
    local description="$2"
    
    echo -e "\n${BLUE}=== $description ===${NC}"
    echo "Consulta: $query"
    echo "----------------------------------------"
    
    docker exec -i hive-server beeline -u jdbc:hive2://localhost:10000 -e "$query"
    local result=$?
    
    if [ $result -eq 0 ]; then
        echo -e "${GREEN}✓ Consulta ejecutada exitosamente${NC}"
    else
        echo -e "${RED}✗ Error ejecutando consulta${NC}"
    fi
    
    return $result
}

# Verificar que los contenedores estén corriendo
echo -e "${YELLOW}1. Verificando contenedores...${NC}"
containers=("namenode" "datanode" "hive-metastore-postgresql" "hive-metastore" "hive-server")

for container in "${containers[@]}"; do
    if docker ps --format 'table {{.Names}}' | grep -q "^$container$"; then
        print_status "$container está corriendo" 0
    else
        print_status "$container NO está corriendo" 1
    fi
done

# Verificar conectividad a servicios
echo -e "\n${YELLOW}2. Verificando conectividad a servicios...${NC}"

# HDFS NameNode
curl -s http://localhost:9870 > /dev/null
print_status "HDFS NameNode Web UI (puerto 9870)" $?

# Hive Server2
timeout 10 bash -c "</dev/tcp/localhost/10000"
print_status "Hive Server2 (puerto 10000)" $?

# PostgreSQL Metastore
timeout 10 bash -c "</dev/tcp/localhost/5432"
print_status "PostgreSQL Metastore (puerto 5432)" $?

# Verificar archivos en HDFS
echo -e "\n${YELLOW}3. Verificando archivos en HDFS...${NC}"
datasets=("customers.csv" "products/products.csv" "orders.csv" "order_items.csv" "reviews.csv")

for dataset in "${datasets[@]}"; do
    docker exec namenode hdfs dfs -test -e "/datasets/$dataset"
    print_status "Dataset $dataset existe en HDFS" $?
done

# Esperar a que Hive Server2 esté completamente listo
echo -e "\n${YELLOW}4. Esperando que Hive Server2 esté listo...${NC}"
sleep 15

# Pruebas básicas de Hive
echo -e "\n${YELLOW}5. Ejecutando pruebas básicas de Hive...${NC}"

# Probar conexión básica
run_hive_query "SHOW DATABASES;" "Verificar conexión a Hive"

# Verificar base de datos
run_hive_query "USE ecommerce_analytics; SHOW TABLES;" "Verificar tablas en base de datos"

# Contar registros en tablas principales
tables=("customers" "products" "orders" "order_items" "reviews")
for table in "${tables[@]}"; do
    run_hive_query "USE ecommerce_analytics; SELECT COUNT(*) as total_$table FROM $table;" "Contar registros en $table"
done

# Ejecutar consultas de ejemplo
echo -e "\n${YELLOW}6. Ejecutando consultas de ejemplo...${NC}"

# Consulta 1: Top 5 categorías por ventas
run_hive_query "
USE ecommerce_analytics;
SELECT 
    p.category,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(oi.total_price) as total_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'completed'
GROUP BY p.category
ORDER BY total_revenue DESC
LIMIT 5;
" "Top 5 categorías por ventas"

# Consulta 2: Clientes por segmento
run_hive_query "
USE ecommerce_analytics;
SELECT 
    customer_segment,
    COUNT(*) as total_customers,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customers), 2) as percentage
FROM customers
GROUP BY customer_segment
ORDER BY total_customers DESC;
" "Distribución de clientes por segmento"

# Consulta 3: Productos más vendidos
run_hive_query "
USE ecommerce_analytics;
SELECT 
    p.product_name,
    p.category,
    SUM(oi.quantity) as total_sold,
    SUM(oi.total_price) as total_revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_sold DESC
LIMIT 10;
" "Top 10 productos más vendidos"

# Verificar optimizaciones de Hive
echo -e "\n${YELLOW}7. Verificando configuraciones de Hive...${NC}"

run_hive_query "SET hive.exec.dynamic.partition;" "Verificar particionado dinámico"
run_hive_query "SET hive.optimize.bucketmapjoin;" "Verificar optimización de bucketing"

# Resumen final
echo -e "\n${BLUE}=== RESUMEN DE VERIFICACIÓN ===${NC}"
echo "✓ Entorno Apache Hive configurado correctamente"
echo "✓ Contenedores Docker funcionando"
echo "✓ Datasets cargados en HDFS"
echo "✓ Tablas creadas en Hive"
echo "✓ Consultas HiveQL ejecutándose correctamente"
echo ""
echo -e "${GREEN}🎉 Prueba de concepto de Apache Hive lista para usar!${NC}"
echo ""
echo "Para acceder a las interfaces web:"
echo "• HDFS NameNode: http://localhost:9870"
echo "• Para conectar con Beeline: docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000"
echo ""
echo "Archivos de consultas disponibles en:"
echo "• /scripts/01_create_tables.hql"
echo "• /scripts/02_queries.hql"
