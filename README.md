# Prueba de Concepto: Apache Hive con Docker

Este proyecto levanta un entorno mínimo de Apache Hive usando Docker, carga un pequeño dataset sintético de ventas y ejecuta consultas HiveQL representativas.

## Estructura del proyecto

- `docker-compose.yml`: Orquesta los contenedores de Hadoop, Hive y dependencias.
- `dataset_ventas.csv`: Dataset sintético de ventas (10 registros de ejemplo).
- `hive_scripts.sql`: Script con creación de tablas y consultas HiveQL.
- `scripts/entrypoint.sh`: Script de inicialización para ejecutar los scripts en Hive.

## Requisitos previos
- Docker y Docker Compose instalados.

## Instrucciones de uso

1. Clona este repositorio y entra a la carpeta del proyecto.
2. Da permisos de ejecución al script de entrada (en Linux/Mac):
   ```sh
   chmod +x scripts/entrypoint.sh
   ```
   En Windows, este paso no es necesario.
3. Levanta el entorno:
   ```sh
   docker-compose up --build
   ```
4. El contenedor `hive-cli` ejecutará automáticamente el script `hive_scripts.sql`.
5. Puedes revisar los resultados de las consultas en los logs del contenedor `hive-cli`:
   ```sh
 docker logs hive-cli
  ```
6. Cuando termines, apaga los contenedores con `Ctrl+C` y ejecuta:
   ```sh
   docker-compose down
   ```

## Personalización
- Puedes modificar el dataset en `dataset_ventas.csv` o las consultas en `hive_scripts.sql` según tus necesidades.

## Notas
- El entorno incluye Hadoop, Hive Metastore, HiveServer2 y una base de datos PostgreSQL para el metastore.
- El dataset es sintético y puede ser reemplazado por uno real o generado con herramientas como Faker.

---

¡Listo para probar Apache Hive de forma sencilla y funcional!
