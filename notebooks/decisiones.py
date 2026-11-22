"""
Decisiones de la entrega final.

Las definimos aquí para no repetirlas en los notebooks 07 a 11. Si se cambia una, hay
que volver a correr esos notebooks. Las de la noche y la gravedad se revisan en el
notebook 08 (prueba G1).
"""

# ---------------------------------------------------------------------------
# Periodo
# ---------------------------------------------------------------------------
# Análisis descriptivo: todo el periodo de la plata (2019-01-01 a 2026-05-31 en choques).
ANIOS_COMPLETOS = list(range(2019, 2026))          # para promedios anuales
# El modelo empieza después del cambio de reporte de la NYPD (marzo-abril de 2020).
MODELO_INICIO = "2021-01-01"
MODELO_FIN_ENTRENAMIENTO = "2024-12-31"            # entrenamiento: 2021-2024
MODELO_FIN_VALIDACION = "2025-12-31"               # validación: 2025 (aquí se eligen parámetros)
MODELO_FIN_PRUEBA = "2026-05-31"                   # prueba: ene-may 2026, solo para evaluar
# Mes desde el que se considera que el reporte cambió (para marcar el quiebre en las figuras)
QUIEBRE_REPORTE = "2020-04-01"

# ---------------------------------------------------------------------------
# Noche y franjas
# ---------------------------------------------------------------------------
# Franjas de 3 horas: 0 = 00-02, 1 = 03-05, ..., 7 = 21-23.
HORAS_POR_FRANJA = 3
# La noche son las franjas 21-23, 00-02 y 03-05 (21:00 a 05:59). Queda alineada con las
# franjas del panel, y el notebook 08 la compara con las horas de mayor letalidad.
FRANJAS_NOCHE = [7, 0, 1]
HORA_INICIO_NOCHE = 21
HORA_FIN_NOCHE = 5            # inclusive
# Una noche pertenece a la fecha en que empieza: un choque del sábado a las 02:00 es de
# la noche del viernes. Por eso se resta un día a las horas de 0 a HORA_CORTE_NOCHE - 1.
HORA_CORTE_NOCHE = 6

# ---------------------------------------------------------------------------
# Gravedad
# ---------------------------------------------------------------------------
# Una persona es KSI (muerta o con lesión seria) si:
#   - PERSON_INJURY = Killed, o
#   - EMOTIONAL_STATUS indica muerte aparente o pérdida de conciencia, o
#   - COMPLAINT describe una lesión seria.
# Son expresiones regulares sobre el texto en mayúsculas. La versión "amplia" agrega
# lesiones intermedias y sirve como análisis de sensibilidad.
KSI_ESTADO_REGEX = r"APPARENT DEATH|UNCONSCIOUS|SEMICONSCIOUS"
KSI_LESION_ESTRICTA_REGEX = (r"SEVERE BLEEDING|FRACTURE|DISLOCATION|INTERNAL|AMPUTATION|PARALYSIS|"
                             r"SEVERE BURN|SEVERE LACERATION|CRUSH")
KSI_LESION_AMPLIA_REGEX = KSI_LESION_ESTRICTA_REGEX + r"|CONCUSSION|MODERATE BURN"
# Cuál de las dos define la variable principal
KSI_VARIANTE = "estricta"     # "estricta" o "amplia"

# ---------------------------------------------------------------------------
# Arrestos que miden control vial
# ---------------------------------------------------------------------------
# LAW_CODE trae el artículo de la ley. VTL = Vehicle and Traffic Law; el 1192 es
# conducir bajo los efectos del alcohol o drogas. Se comparan con la regla anterior
# (OFNS_DESC) en el notebook 08.
DWI_LAW_REGEX = r"^VTL0?1192"
TRANSITO_LAW_REGEX = r"^VTL"
# Los arrestos solo traen fecha (sin hora): el control se mide por barrio y día.

# ---------------------------------------------------------------------------
# Unidad espacial
# ---------------------------------------------------------------------------
UNIDAD_ESPACIAL = "puma"      # 55 barrios (PUMA 2010)

# ---------------------------------------------------------------------------
# Índice de brecha de control
# ---------------------------------------------------------------------------
# Percentil de riesgo menos percentil de control (va de -1 a 1; positivo = más riesgo
# que control). El residuo de log(control) sobre log(riesgo) se calcula como
# comprobación.
BRECHA_METODO = "percentiles"

# ---------------------------------------------------------------------------
# Sensibilidad de las exclusiones
# ---------------------------------------------------------------------------
UMBRAL_SENSIBILIDAD_PP = 2.0  # puntos porcentuales
UMBRAL_SENSIBILIDAD_TOP = 10  # primeros puestos del ranking

# ---------------------------------------------------------------------------
# Objetivo del modelo supervisado
# ---------------------------------------------------------------------------
# "ksi": la celda tiene al menos un choque con una persona KSI.
# "lesion": la celda tiene al menos un choque con lesionados.
# "auto": usa "ksi" si su prevalencia en entrenamiento es >= PREVALENCIA_MINIMA; si no, "lesion".
OBJETIVO = "auto"
PREVALENCIA_MINIMA = 0.01
# Ventanas de la historia reciente (en noches) para las variables de rezago
VENTANAS_REZAGO = [28, 364]

# ---------------------------------------------------------------------------
# Agrupamiento
# ---------------------------------------------------------------------------
K_RANGO = list(range(2, 9))
SEMILLAS = [7, 21, 42, 84, 123]
UMBRAL_CORRELACION = 0.8

# ---------------------------------------------------------------------------
# Capturas de la métrica de negocio
# ---------------------------------------------------------------------------
TOP_CELDAS = 0.10             # "si se controla el 10 % de celdas que marca el modelo..."

# Un grupo entra a un ranking de proporciones solo si tiene al menos estos casos
MINIMO_CASOS_RANKING = 200

# Textos que significan que el choque no tiene causa registrada. "DOES NOT APPLY" apareció con
# la republicación del 28-oct-2026.
VALORES_SIN_CAUSA = ["", "UNSPECIFIED", "UNKNOWN", "DOES NOT APPLY"]

SEMILLA = 42
