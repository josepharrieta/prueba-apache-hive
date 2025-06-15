#!/bin/bash
set -e

# Esperar a que el servidor de Hive esté listo
sleep 30

# Ejecutar el script de HiveQL
beeline -u "jdbc:hive2://hive-server:10000/default" -n hive -f /hive_scripts.sql
