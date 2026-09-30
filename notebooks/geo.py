"""
Funciones para ubicar puntos (latitud, longitud) en su PUMA 2010 y su borough con Spark.

Cómo funciona el spatial join:
  1. El driver lee las 55 PUMAs del GeoJSON y revisa que estén bien.
  2. Los polígonos se mandan una sola vez a cada executor con un broadcast.
  3. En cada executor se arma un STRtree (índice espacial de shapely) y, con una
     pandas UDF, se busca por lotes de filas en qué polígono cae cada punto.
Así no hace falta un join cartesiano y los millones de puntos se procesan en paralelo.
"""
import json

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

from config import LAT_MAX, LAT_MIN, LON_MAX, LON_MIN, RUTA_PUMA

# En la numeración de las PUMAs 2010 los dos primeros dígitos dicen el borough.
# cargar_pumas() revisa que la cantidad por borough cuadre con esto.
BOROUGH_POR_PREFIJO = {"37": "Bronx", "38": "Manhattan", "39": "Staten Island", "40": "Brooklyn", "41": "Queens"}
PUMAS_ESPERADAS = {"Bronx": 10, "Manhattan": 10, "Staten Island": 3, "Brooklyn": 18, "Queens": 14}


def borough_de_puma(puma):
    return BOROUGH_POR_PREFIJO.get(str(puma)[:2])


def cargar_pumas(spark, ruta=RUTA_PUMA):
    """Lee el GeoJSON desde el almacenamiento del cluster y devuelve un DataFrame de
    pandas con puma, borough, área y la geometría en WKB. Si algo no cuadra, falla."""
    from shapely.geometry import shape

    texto = spark.read.text(ruta, wholetext=True).first()[0]
    geo = json.loads(texto)
    filas = []
    for f in geo["features"]:
        geom = shape(f["geometry"])
        puma = str(f["properties"]["puma"]).zfill(4)
        filas.append({
            "puma": puma,
            "borough": borough_de_puma(puma),
            "shape_area": float(f["properties"].get("shape_area") or "nan"),
            "wkb": geom.wkb,
            "lon_c": geom.centroid.x,
            "lat_c": geom.centroid.y,
        })
    pumas = pd.DataFrame(filas)

    # Si algo de esto falla preferimos que se detenga a que asigne mal los barrios
    assert len(pumas) == 55, f"Se esperaban 55 PUMAs y hay {len(pumas)}"
    assert pumas["borough"].notna().all(), "Hay PUMAs con un código que no corresponde a ningún borough"
    conteo = pumas["borough"].value_counts().to_dict()
    assert conteo == PUMAS_ESPERADAS, f"PUMAs por borough inesperadas: {conteo}"
    assert pumas["lon_c"].between(LON_MIN, LON_MAX).all() and pumas["lat_c"].between(LAT_MIN, LAT_MAX).all(), \
        "Las coordenadas de las fronteras no están en grados (lon/lat) sobre Nueva York"
    return pumas


def coordenada_valida(lat, lon):
    """Expresión de Spark que da True si el punto está dentro de la caja de Nueva York."""
    return lat.between(LAT_MIN, LAT_MAX) & lon.between(LON_MIN, LON_MAX)


def agregar_puma(df, pumas, col_lat="lat", col_lon="lon", col_salida="puma"):
    """Agrega la columna `col_salida` con la PUMA donde cae cada punto. Queda nula si
    no hay coordenadas o si el punto no cae en ninguna PUMA (por ejemplo, en el agua)."""
    spark = df.sparkSession
    wkbs = spark.sparkContext.broadcast(list(zip(pumas["puma"], pumas["wkb"])))

    @pandas_udf("string")
    def _puma(lat: pd.Series, lon: pd.Series) -> pd.Series:
        # Esto corre en los executors, un lote de filas a la vez. Con 55 polígonos el
        # índice se arma en milisegundos, así que no pasa nada por rehacerlo en cada lote.
        import numpy as np
        import shapely
        codigos = np.array([c for c, _ in wkbs.value])
        arbol = shapely.STRtree(shapely.from_wkb([w for _, w in wkbs.value]))
        salida = np.full(len(lat), None, dtype=object)
        ok = (lat.notna() & lon.notna()).to_numpy()
        if ok.any():
            puntos = shapely.points(lon.to_numpy()[ok], lat.to_numpy()[ok])
            idx_punto, idx_poly = arbol.query(puntos, predicate="intersects")
            asignados = np.full(ok.sum(), None, dtype=object)
            # si un punto toca dos PUMAs (frontera), se queda con la primera
            asignados[idx_punto[::-1]] = codigos[idx_poly[::-1]]
            salida[ok] = asignados
        return pd.Series(salida)

    return df.withColumn(col_salida, _puma(F.col(col_lat).cast("double"), F.col(col_lon).cast("double")))


def borough_desde_puma(col_puma):
    """Expresión de Spark con el nombre del borough según el código de PUMA."""
    mapa = F.create_map(*[F.lit(x) for kv in BOROUGH_POR_PREFIJO.items() for x in kv])
    return mapa[F.substring(col_puma, 1, 2)]
