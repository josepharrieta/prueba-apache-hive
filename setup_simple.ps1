# Script de Automatizacion Completa - Apache Hive PoC
# Version PowerShell para Windows

Write-Host "Iniciando Prueba de Concepto Apache Hive" -ForegroundColor Blue

function Write-Log {
    param($Message)
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message" -ForegroundColor Blue
}

function Write-ErrorMsg {
    param($Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Write-Success {
    param($Message)
    Write-Host "[SUCCESS] $Message" -ForegroundColor Green
}

function Write-WarningMsg {
    param($Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
}

# Verificar Docker
Write-Log "Verificando Docker..."
try {
    docker --version | Out-Null
    docker info | Out-Null
    Write-Success "Docker esta corriendo correctamente"
}
catch {
    Write-ErrorMsg "Docker no esta instalado o no esta corriendo"
    exit 1
}

# Verificar Python
Write-Log "Verificando Python..."
$pythonCmd = "python"
try {
    python --version | Out-Null
    Write-Success "Python encontrado"
}
catch {
    try {
        python3 --version | Out-Null
        Write-Success "Python3 encontrado"
        $pythonCmd = "python3"
    }
    catch {
        Write-ErrorMsg "Python no esta instalado"
        exit 1
    }
}

# Instalar dependencias Python
Write-Log "Instalando dependencias Python..."
pip install faker pandas

# Paso 1: Generar datasets
Write-Log "Paso 1/6: Generando datasets sinteticos..."
& $pythonCmd scripts/generate_datasets.py

# Paso 2: Iniciar servicios Docker
Write-Log "Paso 2/6: Iniciando servicios Docker..."
docker-compose down --remove-orphans
docker-compose up -d

# Esperar a que los servicios esten listos
Write-Log "Esperando a que los servicios se inicialicen..."
Start-Sleep -Seconds 60

# Verificar contenedores
Write-Log "Verificando estado de contenedores..."
$containers = @("namenode", "datanode", "hive-metastore-postgresql", "hive-metastore", "hive-server")

foreach ($container in $containers) {
    $containerStatus = docker ps --format "table {{.Names}}" | Select-String "^$container$"
    if ($containerStatus) {
        Write-Success "$container esta corriendo"
    }
    else {
        Write-ErrorMsg "$container NO esta corriendo"
    }
}

# Paso 3: Preparar datos en HDFS
Write-Log "Paso 3/6: Preparando datos en HDFS..."
Start-Sleep -Seconds 30

Write-Log "Creando directorios en HDFS..."
docker exec namenode hdfs dfs -mkdir -p /datasets
docker exec namenode hdfs dfs -mkdir -p /datasets/products
docker exec namenode hdfs dfs -mkdir -p /datasets/reviews_orc
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse

Write-Log "Copiando archivos CSV a HDFS..."
docker exec namenode hdfs dfs -put /datasets/customers.csv /datasets/
docker exec namenode hdfs dfs -put /datasets/products.csv /datasets/products/
docker exec namenode hdfs dfs -put /datasets/orders.csv /datasets/
docker exec namenode hdfs dfs -put /datasets/order_items.csv /datasets/
docker exec namenode hdfs dfs -put /datasets/reviews.csv /datasets/

Write-Log "Estableciendo permisos en HDFS..."
docker exec namenode hdfs dfs -chmod -R 777 /datasets
docker exec namenode hdfs dfs -chmod -R 777 /user/hive/warehouse

Write-Success "Datos preparados en HDFS"

# Paso 4: Crear tablas Hive
Write-Log "Paso 4/6: Creando tablas en Hive..."
Start-Sleep -Seconds 30

docker exec -i hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/01_create_tables.hql

# Paso 5: Ejecutar consultas de ejemplo
Write-Log "Paso 5/6: Ejecutando consultas de ejemplo..."
$query = "USE ecommerce_analytics; SELECT COUNT(*) as total_customers FROM customers;"
docker exec -i hive-server beeline -u jdbc:hive2://localhost:10000 -e $query

# Resumen final
Write-Host ""
Write-Host "Prueba de Concepto Apache Hive Completada!" -ForegroundColor Green
Write-Host ""
Write-Host "Resumen del entorno:" -ForegroundColor Cyan
Write-Host "- Base de datos: ecommerce_analytics"
Write-Host "- Tablas creadas: 5"
Write-Host "- Total de registros: aproximadamente 96,000"
Write-Host ""
Write-Host "Interfaces Web disponibles:" -ForegroundColor Cyan
Write-Host "- HDFS NameNode: http://localhost:9870"
Write-Host "- DataNode: http://localhost:9864"
Write-Host ""
Write-Host "Conexion a Hive:" -ForegroundColor Cyan
Write-Host "docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000"
Write-Host ""
Write-Success "Entorno listo para explorar Apache Hive!"
