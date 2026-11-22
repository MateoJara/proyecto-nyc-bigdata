"""
Configuración que comparten todos los notebooks.

Las rutas de los datos solo se definen aquí. Para correr el proyecto en otro lado
(Databricks, por ejemplo) basta con poner la variable de entorno NYC_RUTA_BASE.
"""
import os

from pyspark.sql import SparkSession

# Rutas de cada capa
RUTA_BASE = os.environ.get("NYC_RUTA_BASE", "/nyc")   # en el cluster esto queda en HDFS
RAW = f"{RUTA_BASE}/raw"          # CSV y GeoJSON tal cual se descargaron
BRONZE = f"{RUTA_BASE}/bronze"    # lo mismo pero en Parquet, todo como texto
SILVER = f"{RUTA_BASE}/silver"    # con tipos, filtrado y limpio
GOLD = f"{RUTA_BASE}/gold"        # agregados para el análisis y los modelos

RUTA_PUMA = f"{RAW}/puma_2010/puma_2010.geojson"

# Entrega final. Va en rutas propias: NYC republicó choques, vehículos y personas el 28-oct-2026
# con identificadores nuevos (texto), que no cruzan con los de la primera entrega (números).
RAW_FINAL = f"{RAW}/entrega_final"
BRONZE_FINAL = f"{BRONZE}/entrega_final"
SILVER_FINAL = f"{SILVER}/entrega_final"
GOLD_FINAL = f"{GOLD}/entrega_final"

# Periodo de análisis: 2019 a 2025 completos más el primer semestre de 2026
FECHA_INICIO = "2019-01-01"
FECHA_FIN = "2026-06-30"
FECHA_FIN_CHOQUES = "2026-05-31"   # último mes completo de choques (corte fijo de la primera entrega)

# Caja aproximada alrededor de los cinco boroughs, con un poco de margen.
# Sirve para descartar coordenadas que no tienen sentido.
LAT_MIN, LAT_MAX = 40.49, 40.92
LON_MIN, LON_MAX = -74.27, -73.68


def sesion(nombre):
    """Crea la SparkSession. El master, la memoria y los puertos ya están en
    spark-defaults.conf, por eso aquí solo va el nombre de la aplicación."""
    return SparkSession.builder.appName(nombre).getOrCreate()
