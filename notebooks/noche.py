"""
Funciones de la entrega final ("noches peligrosas").

Hay dos tipos de funciones:
  - Expresiones de Spark (franja, noche, KSI, arrestos de control) que usan los notebooks.
  - Funciones de Python puro (calendario, métricas, índice de Rand) que se pueden probar
    sin el cluster: tests/probar_noche.py las revisa con casos conocidos.
"""
import datetime as dt

from decisiones import (DWI_LAW_REGEX, FRANJAS_NOCHE, HORA_CORTE_NOCHE, HORAS_POR_FRANJA,
                        KSI_ESTADO_REGEX, KSI_LESION_AMPLIA_REGEX, KSI_LESION_ESTRICTA_REGEX,
                        TRANSITO_LAW_REGEX, VALORES_SIN_CAUSA)

ETIQUETAS_FRANJA = {i: f"{i * HORAS_POR_FRANJA:02d}-{i * HORAS_POR_FRANJA + HORAS_POR_FRANJA - 1:02d}"
                    for i in range(24 // HORAS_POR_FRANJA)}
ORDEN_FRANJAS_NOCHE = FRANJAS_NOCHE            # 21-23, 00-02, 03-05 en el orden en que pasan

# Nombres de las noches según el día en que empiezan (la noche del viernes va del viernes
# a las 21:00 al sábado a las 05:59)
NOCHES = {"Lunes": "Lun→Mar", "Martes": "Mar→Mié", "Miércoles": "Mié→Jue", "Jueves": "Jue→Vie",
          "Viernes": "Vie→Sáb", "Sábado": "Sáb→Dom", "Domingo": "Dom→Lun"}


# ---------------------------------------------------------------------------
# Expresiones de Spark
# ---------------------------------------------------------------------------
def col_franja(hora):
    """Franja de 3 horas (0 a 7) a partir de la hora (0 a 23)."""
    from pyspark.sql import functions as F
    return F.floor(hora / HORAS_POR_FRANJA).cast("int")


def col_es_noche(hora):
    return col_franja(hora).isin(FRANJAS_NOCHE)


def col_fecha_noche(fecha, hora):
    """Fecha en que empieza la noche: las horas de 0 a 5 pertenecen a la noche anterior."""
    from pyspark.sql import functions as F
    return F.when(hora < HORA_CORTE_NOCHE, F.date_sub(fecha, 1)).otherwise(fecha)


def col_ksi_persona(person_injury, emotional_status, complaint, variante="estricta"):
    """True si la persona murió o quedó con una lesión seria (ver decisiones.py)."""
    from pyspark.sql import functions as F
    lesion = KSI_LESION_ESTRICTA_REGEX if variante == "estricta" else KSI_LESION_AMPLIA_REGEX
    muerto = F.coalesce(F.upper(F.trim(person_injury)) == "KILLED", F.lit(False))
    estado = F.coalesce(F.upper(emotional_status).rlike(KSI_ESTADO_REGEX), F.lit(False))
    seria = F.coalesce(F.upper(complaint).rlike(lesion), F.lit(False))
    return muerto | estado | seria


def col_sin_causa(factor):
    """True si el factor no dice ninguna causa."""
    from pyspark.sql import functions as F
    return F.coalesce(F.upper(F.trim(factor)), F.lit("")).isin(VALORES_SIN_CAUSA)


def col_arresto_dwi(law_code):
    from pyspark.sql import functions as F
    return F.coalesce(F.upper(F.regexp_replace(law_code, r"\s", "")).rlike(DWI_LAW_REGEX), F.lit(False))


def col_arresto_transito(law_code):
    from pyspark.sql import functions as F
    return F.coalesce(F.upper(F.regexp_replace(law_code, r"\s", "")).rlike(TRANSITO_LAW_REGEX), F.lit(False))


# ---------------------------------------------------------------------------
# Calendario (Python puro)
# ---------------------------------------------------------------------------
def _n_esimo(anio, mes, dia_semana, n):
    """n-ésimo día de la semana del mes (lunes=0). n=-1 es el último."""
    if n > 0:
        d = dt.date(anio, mes, 1)
        d += dt.timedelta(days=(dia_semana - d.weekday()) % 7)
        return d + dt.timedelta(weeks=n - 1)
    siguiente = dt.date(anio + (mes == 12), mes % 12 + 1, 1)
    d = siguiente - dt.timedelta(days=1)
    return d - dt.timedelta(days=(d.weekday() - dia_semana) % 7)


def _observado(d):
    """Festivo que cae en sábado se observa el viernes; en domingo, el lunes."""
    if d.weekday() == 5:
        return d - dt.timedelta(days=1)
    if d.weekday() == 6:
        return d + dt.timedelta(days=1)
    return d


# Super Bowl LIII a LX (domingo)
SUPER_BOWL = ["2019-02-03", "2020-02-02", "2021-02-07", "2022-02-13", "2023-02-12",
              "2024-02-11", "2025-02-09", "2026-02-08"]


def calendario(anio_inicio, anio_fin):
    """Lista de dicts {fecha, nombre, tipo} con festivos federales (fecha observada) y
    noches de eventos con mucho consumo de alcohol. `tipo` es "festivo" o "evento".
    La fecha de un evento es la fecha en que EMPIEZA esa noche."""
    filas = []
    for a in range(anio_inicio, anio_fin + 1):
        festivos = {
            "Año Nuevo": _observado(dt.date(a, 1, 1)),
            "Martin Luther King": _n_esimo(a, 1, 0, 3),
            "Día de los Presidentes": _n_esimo(a, 2, 0, 3),
            "Memorial Day": _n_esimo(a, 5, 0, -1),
            "Independencia": _observado(dt.date(a, 7, 4)),
            "Día del Trabajo": _n_esimo(a, 9, 0, 1),
            "Columbus Day": _n_esimo(a, 10, 0, 2),
            "Día de los Veteranos": _observado(dt.date(a, 11, 11)),
            "Acción de Gracias": _n_esimo(a, 11, 3, 4),
            "Navidad": _observado(dt.date(a, 12, 25)),
        }
        if a >= 2021:                                   # Juneteenth es festivo federal desde 2021
            festivos["Juneteenth"] = _observado(dt.date(a, 6, 19))
        for nombre, f in festivos.items():
            filas.append({"fecha": f, "nombre": nombre, "tipo": "festivo"})

        accion_gracias = festivos["Acción de Gracias"]
        eventos = {
            "Nochevieja": dt.date(a, 12, 31),
            "Víspera de Independencia": dt.date(a, 7, 3),
            "Noche de Independencia": dt.date(a, 7, 4),
            "Halloween": dt.date(a, 10, 31),
            "San Patricio": dt.date(a, 3, 17),
            "Víspera de Acción de Gracias": accion_gracias - dt.timedelta(days=1),
            "Víspera de Memorial Day": festivos["Memorial Day"] - dt.timedelta(days=1),
            "Víspera del Día del Trabajo": festivos["Día del Trabajo"] - dt.timedelta(days=1),
        }
        for nombre, f in eventos.items():
            filas.append({"fecha": f, "nombre": nombre, "tipo": "evento"})
    for s in SUPER_BOWL:
        f = dt.date.fromisoformat(s)
        if anio_inicio <= f.year <= anio_fin:
            filas.append({"fecha": f, "nombre": "Super Bowl", "tipo": "evento"})
    return filas


# ---------------------------------------------------------------------------
# Métricas (Python puro, sobre arreglos de numpy)
# ---------------------------------------------------------------------------
def rangos(v):
    """Rangos de 1 a n con empates promediados (como scipy.stats.rankdata)."""
    import numpy as np
    v = np.asarray(v, dtype=float)
    orden = np.argsort(v, kind="mergesort")
    v_ord = v[orden]
    nuevo = np.r_[True, v_ord[1:] != v_ord[:-1]]
    grupo = np.cumsum(nuevo) - 1
    inicio = np.flatnonzero(nuevo)
    fin = np.r_[inicio[1:], len(v)]
    promedio = (inicio + fin + 1) / 2.0           # rango medio de cada grupo de empates
    r = np.empty(len(v))
    r[orden] = promedio[grupo]
    return r


def captura_top(y, puntaje, fraccion):
    """Proporción de los positivos que quedan dentro de la fracción de celdas con mayor
    puntaje. Los empates en el corte se reparten en proporción (así un puntaje constante
    da `fraccion` y no un número arbitrario)."""
    import numpy as np
    y = np.asarray(y, dtype=float)
    p = np.asarray(puntaje, dtype=float)
    total = y.sum()
    if total == 0:
        return float("nan")
    k = fraccion * len(y)
    orden = np.argsort(-p, kind="mergesort")
    p_ord, y_ord = p[orden], y[orden]
    umbral = p_ord[int(np.ceil(k)) - 1] if k >= 1 else p_ord[0]
    arriba = p_ord > umbral
    empate = p_ord == umbral
    cupo = k - arriba.sum()
    capturados = y_ord[arriba].sum() + (y_ord[empate].sum() * cupo / empate.sum() if empate.any() else 0)
    return float(capturados / total)


def auc_roc(y, puntaje):
    """AUC-ROC con empates (estadístico de Mann-Whitney)."""
    import numpy as np
    y = np.asarray(y, dtype=int)
    r = rangos(np.asarray(puntaje, dtype=float))
    n1, n0 = y.sum(), len(y) - y.sum()
    if n1 == 0 or n0 == 0:
        return float("nan")
    return float((r[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def auc_pr(y, puntaje):
    """Precisión promedio (área bajo la curva de precisión y sensibilidad, por pasos).
    Los empates se tratan como un solo umbral."""
    import numpy as np
    y = np.asarray(y, dtype=float)
    p = np.asarray(puntaje, dtype=float)
    if y.sum() == 0:
        return float("nan")
    orden = np.argsort(-p, kind="mergesort")
    p, y = p[orden], y[orden]
    vp = np.cumsum(y)
    n = np.arange(1, len(y) + 1)
    ultimo = np.r_[p[1:] != p[:-1], True]           # último índice de cada umbral
    vp, n = vp[ultimo], n[ultimo]
    precision = vp / n
    sensibilidad = vp / y.sum()
    delta = np.diff(np.r_[0.0, sensibilidad])
    return float((precision * delta).sum())


def indice_rand_ajustado(a, b):
    """Índice de Rand ajustado entre dos particiones (1 = idénticas, ~0 = al azar)."""
    import numpy as np
    a = np.asarray(a)
    b = np.asarray(b)
    _, ia = np.unique(a, return_inverse=True)
    _, ib = np.unique(b, return_inverse=True)
    tabla = np.zeros((ia.max() + 1, ib.max() + 1), dtype=np.int64)
    np.add.at(tabla, (ia, ib), 1)
    comb = lambda x: x * (x - 1) / 2
    suma_ij = comb(tabla).sum()
    suma_a = comb(tabla.sum(axis=1)).sum()
    suma_b = comb(tabla.sum(axis=0)).sum()
    esperado = suma_a * suma_b / comb(len(a))
    maximo = (suma_a + suma_b) / 2
    if maximo == esperado:
        return 1.0
    return float((suma_ij - esperado) / (maximo - esperado))


def percentil(valores):
    """Percentil (0 a 1) de cada valor dentro del arreglo, con empates promediados."""
    import numpy as np
    v = np.asarray(valores, dtype=float)
    if len(v) == 1:
        return np.array([0.5])
    return (rangos(v) - 1) / (len(v) - 1)


def spearman(x, y):
    """Correlación de Spearman sin scipy (Pearson de los rangos)."""
    import numpy as np
    return float(np.corrcoef(rangos(np.asarray(x, float)), rangos(np.asarray(y, float)))[0, 1])


# ---------------------------------------------------------------------------
# Lectura (igual que en 01_ingesta, para las fuentes nuevas)
# ---------------------------------------------------------------------------
def normalizar_columnas(df):
    """Nombres en minúscula y con guion bajo ("CRASH DATE" -> "crash_date")."""
    import re
    vistos, nuevos = {}, []
    for c in df.columns:
        n = re.sub(r"[^0-9a-z]+", "_", c.strip().lower()).strip("_") or "columna"
        if n in vistos:
            vistos[n] += 1
            n = f"{n}_{vistos[n]}"
        else:
            vistos[n] = 0
        nuevos.append(n)
    return df.toDF(*nuevos)


def leer_csv_crudo(spark, ruta):
    """CSV como texto, sin inferir tipos (los tipos se ponen en plata)."""
    df = (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
          .option("inferSchema", False).csv(ruta))
    return normalizar_columnas(df)


def col_fecha(texto):
    """Fecha desde el texto ISO que entrega la API de NYC Open Data (2024-05-09T00:00:00.000).
    Con ANSI activo to_date falla con texto inválido; try_to_timestamp da nulo."""
    from pyspark.sql import functions as F
    return F.to_date(F.try_to_timestamp(F.substring(F.trim(texto), 1, 10), F.lit("yyyy-MM-dd")))


def nombres_puma(ruta="../fuentes/nombres_puma.csv"):
    """Nombre de cada barrio PUMA (el de sus vecindarios principales)."""
    import pandas as pd
    return pd.read_csv(ruta, dtype={"puma": str})


def horas_locales(om, inicio, fin, zona="America/New_York"):
    """Pasa la serie horaria de UTC a la hora de Nueva York y la recorta al periodo.

    Devuelve (tabla con una fila por hora local que existe, número de horas sin dato en la fuente).
    En marzo la hora 02 no existe y no se inventa; en noviembre la 01 se repite y queda la primera."""
    import pandas as pd
    d = om.copy()
    d["ts_local"] = pd.to_datetime(d["time"], utc=True).dt.tz_convert(zona).dt.tz_localize(None)
    d = d.sort_values("time").drop_duplicates("ts_local").set_index("ts_local")
    desde = pd.Timestamp(inicio).tz_localize(zona)
    hasta = (pd.Timestamp(fin) + pd.Timedelta(days=1)).tz_localize(zona)
    utc = pd.date_range(desde.tz_convert("UTC"), hasta.tz_convert("UTC"), freq="h", inclusive="left")
    esperadas = utc.tz_convert(zona).tz_localize(None).unique().rename("ts_local")
    faltan = len(esperadas.difference(d.index))
    return d.reindex(esperadas).reset_index(), faltan


def wilson(exitos, n, z=1.96):
    """Intervalo de Wilson al 95 % para una proporción. Acepta números o arreglos."""
    import numpy as np
    exitos = np.asarray(exitos, dtype=float)
    n = np.asarray(n, dtype=float)
    with np.errstate(invalid="ignore", divide="ignore"):
        p = exitos / n
        centro = (p + z * z / (2 * n)) / (1 + z * z / n)
        margen = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return centro - margen, centro + margen
