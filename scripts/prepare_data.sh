#!/bin/bash

# =====================================================
# Script de preparación de datos para Apache Hive
# =====================================================

echo "=== Preparación de datos para Apache Hive ==="

# Configurar variables
HADOOP_USER="root"
HDFS_DATASETS_DIR="/datasets"

echo "Esperando que los servicios estén listos..."
sleep 30

# Función para ejecutar comandos en HDFS
run_hdfs_command() {
    echo "Ejecutando: $1"
    docker exec namenode $1
    if [ $? -eq 0 ]; then
        echo "✓ Comando ejecutado exitosamente"
    else
        echo "✗ Error ejecutando comando"
        return 1
    fi
}

# Crear directorios en HDFS
echo "Creando directorios en HDFS..."
run_hdfs_command "hdfs dfs -mkdir -p /datasets"
run_hdfs_command "hdfs dfs -mkdir -p /datasets/products"
run_hdfs_command "hdfs dfs -mkdir -p /datasets/reviews_orc"
run_hdfs_command "hdfs dfs -mkdir -p /user/hive/warehouse"

# Copiar archivos CSV a HDFS
echo "Copiando archivos CSV a HDFS..."
run_hdfs_command "hdfs dfs -put /datasets/customers.csv /datasets/"
run_hdfs_command "hdfs dfs -put /datasets/products.csv /datasets/products/"
run_hdfs_command "hdfs dfs -put /datasets/orders.csv /datasets/"
run_hdfs_command "hdfs dfs -put /datasets/order_items.csv /datasets/"
run_hdfs_command "hdfs dfs -put /datasets/reviews.csv /datasets/"

# Establecer permisos
echo "Estableciendo permisos en HDFS..."
run_hdfs_command "hdfs dfs -chmod -R 777 /datasets"
run_hdfs_command "hdfs dfs -chmod -R 777 /user/hive/warehouse"

# Verificar archivos
echo "Verificando archivos en HDFS..."
run_hdfs_command "hdfs dfs -ls /datasets"
run_hdfs_command "hdfs dfs -ls /datasets/products"

echo "=== Preparación de datos completada ==="
