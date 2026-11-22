# %% [markdown]
# # 09 - Capa oro: filtros, transformaciones y tablas agregadas
#
# Aquí armamos las tablas de donde salen las respuestas (notebook 10) y los modelos (notebook 11). Cada filtro y cada transformación queda anotado con su efecto en `tabla_09_filtros_transformaciones.csv`, para el numeral 9 del documento.
#
# | Tabla oro | Una fila por | Para qué |
# |---|---|---|
# | `choques_enriquecidos` | choque | Letalidad, causas y usuarios por franja (P3, P4, P7, P8) |
# | `personas_enriquecidas` | persona | Quiénes mueren o quedan con lesiones serias (P4) |
# | `panel_noche` | barrio × noche × franja nocturna | Brecha, estabilidad y modelo predictivo (P5, P6, P7, P9) |
# | `barrios_noche` | barrio | Brecha de control y agrupamiento (P6, P8, K-Means) |
#
# Los filtros definen qué entra en cada tabla oro y los registros de la plata se conservan todos.

# %%
import time

import numpy as np
import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

import noche
from codigos import DIAS_SEMANA, GRUPOS_CAUSA, GRUPOS_VEHICULO, SIN_CAUSA, VEHICULO_OTRO
from config import FECHA_INICIO, GOLD_FINAL, SILVER, SILVER_FINAL, sesion
from estilo import dec, miles
from decisiones import ANIOS_COMPLETOS, FRANJAS_NOCHE, MODELO_INICIO, VENTANAS_REZAGO

spark = sesion("09_capa_oro")
t_inicio = time.time()
registro = []

def anotar(tipo, tabla, procedimiento, justificacion, antes=None, despues=None, efecto=None):
    if efecto is None and antes is not None:
        efecto = f"{miles(antes)} → {miles(despues)} ({dec(100 * despues / antes)} % se conserva)"
    registro.append({"tipo": tipo, "tabla": tabla, "procedimiento": procedimiento,
                     "justificacion": justificacion, "efecto": efecto})
    print(f"[{tipo}] {tabla}: {procedimiento} -> {efecto}")

def mapa(diccionario):
    return F.create_map(*[F.lit(x) for kv in diccionario.items() for x in kv])

choques = spark.read.parquet(f"{SILVER_FINAL}/choques")
personas = spark.read.parquet(f"{SILVER_FINAL}/personas").filter("tiene_choque")
vehiculos = spark.read.parquet(f"{SILVER_FINAL}/vehiculos")
arrestos = spark.read.parquet(f"{SILVER}/arrestos")
clima = spark.read.parquet(f"{SILVER_FINAL}/clima_hora")
calendario = spark.read.parquet(f"{SILVER_FINAL}/calendario")
poblacion = spark.read.parquet(f"{SILVER_FINAL}/poblacion_puma_2020")
corte = choques.agg(F.max("fecha")).first()[0]
print("Último día con choques:", corte)

# %% [markdown]
# ## 1. Grupos de causa y de vehículo
#
# Las causas se agrupan con las expresiones de `codigos.py` y se toma el primer grupo que coincide. Aparte marcamos alcohol o drogas y velocidad o agresividad mirando hasta cinco factores del choque, porque el alcohol puede venir como segundo factor.

# %%
def grupo_por_regex(columna, grupos, otro, sin_dato):
    texto = F.upper(F.trim(columna))
    expr = None
    for nombre, regex in grupos:
        cond = texto.rlike(regex)
        expr = F.when(cond, nombre) if expr is None else expr.when(cond, nombre)
    vacio = noche.col_sin_causa(columna)
    return F.when(vacio, sin_dato).otherwise(expr.otherwise(otro))

ALCOHOL = r"ALCOHOL|DRUG|PRESCRIPTION"
VELOCIDAD = r"UNSAFE SPEED|AGGRESSIVE"
factores = [F.upper(F.coalesce(F.col(f"factor_{i}"), F.lit(""))) for i in range(1, 6)]

# Factores de los vehículos (la tabla de vehículos también trae los suyos)
fact_veh = (vehiculos.groupBy("collision_id").agg(
    F.max(F.upper(F.concat_ws(" ", "factor_1", "factor_2")).rlike(ALCOHOL).cast("int")).alias("alcohol_veh"),
    F.max(F.upper(F.concat_ws(" ", "factor_1", "factor_2")).rlike(VELOCIDAD).cast("int")).alias("velocidad_veh")))

veh = vehiculos.withColumn("grupo_vehiculo", grupo_por_regex(F.col("tipo_vehiculo"), GRUPOS_VEHICULO, VEHICULO_OTRO, "Sin dato"))
banderas_veh = veh.groupBy("collision_id").agg(
    F.count("*").alias("n_vehiculos"),
    *[F.max((F.col("grupo_vehiculo") == g).cast("int")).alias(n) for n, g in
      [("hay_moto", "Motocicleta"), ("hay_bici", "Bicicleta"), ("hay_micromovilidad", "Micromovilidad eléctrica"),
       ("hay_camion", "Camión"), ("hay_bus", "Bus"), ("hay_taxi", "Taxi")]])

# %% [markdown]
# ## 2. Personas: tipo de usuario
#
# `person_type` dice si la persona era peatón, ciclista u ocupante. Para los ocupantes buscamos el tipo de su vehículo con `vehicle_id`, que es la llave hacia `unique_id` de vehículos según la documentación de la fuente.

# %%
p_ids = personas.filter(F.col("vehicle_id").isNotNull()).select("vehicle_id").distinct()
v_tipo = vehiculos.select(F.col("unique_id").alias("vehicle_id"), "tipo_vehiculo").dropDuplicates(["vehicle_id"])
n_ids, n_enlazados = p_ids.count(), p_ids.join(v_tipo, "vehicle_id", "left_semi").count()
tipo_por_id = (v_tipo.withColumn("grupo_vehiculo", grupo_por_regex(F.col("tipo_vehiculo"), GRUPOS_VEHICULO, VEHICULO_OTRO, "Sin dato"))
               .select("vehicle_id", "grupo_vehiculo"))
anotar("Transformación", "personas", "Tipo de vehículo del ocupante enlazando vehicle_id con unique_id de vehículos",
       "Separar motociclistas y usuarios de micromovilidad de los demás ocupantes",
       efecto=f"{dec(100 * n_enlazados / max(n_ids, 1))} % de los vehicle_id enlazados")

tp = F.upper(F.trim("tipo_persona"))
personas_e = (personas.join(tipo_por_id, "vehicle_id", "left")
    .withColumn("tipo_usuario",
        F.when(tp == "PEDESTRIAN", "Peatón")
         .when(tp == "BICYCLIST", "Ciclista")
         .when(tp == "OCCUPANT", F.when(F.col("grupo_vehiculo") == "Motocicleta", "Motociclista")
                                  .when(F.col("grupo_vehiculo") == "Micromovilidad eléctrica", "Micromovilidad")
                                  .when(F.col("grupo_vehiculo") == "Bicicleta", "Ciclista")
                                  .otherwise("Ocupante de otro vehículo"))
         .when(tp.contains("MOTORIZED"), "Micromovilidad")
         .otherwise("Otro o sin dato")))

ksi_por_choque = personas_e.groupBy("collision_id").agg(
    F.max(F.col("ksi").cast("int")).alias("ksi"),
    F.max(F.col("ksi_amplia").cast("int")).alias("ksi_amplia"),
    F.sum(F.col("ksi").cast("int")).alias("personas_ksi"),
    F.sum((F.col("ksi") & (F.col("tipo_usuario") == "Peatón")).cast("int")).alias("peatones_ksi"),
    F.sum((F.col("ksi") & F.col("tipo_usuario").isin("Ciclista", "Micromovilidad")).cast("int")).alias("ciclistas_micro_ksi"),
    F.sum((F.col("ksi") & (F.col("tipo_usuario") == "Motociclista")).cast("int")).alias("motociclistas_ksi"),
    F.max(F.upper(F.concat_ws(" ", "factor_persona_1", "factor_persona_2")).rlike(ALCOHOL).cast("int")).alias("alcohol_persona"))

# %% [markdown]
# ## 3. Choques enriquecidos

# %%
cal_dia = calendario.groupBy("fecha").agg(
    F.max((F.col("tipo") == "festivo").cast("int")).alias("es_festivo"),
    F.max((F.col("tipo") == "evento").cast("int")).alias("es_evento"),
    F.concat_ws(", ", F.collect_set("nombre")).alias("nombre_fecha"))

clima_hora = clima.select("fecha", "hora", "temperatura", "precipitacion_mm", "llueve", "nieva", "viento_kmh")

ce = (choques
    .join(ksi_por_choque, "collision_id", "left")
    .join(banderas_veh, "collision_id", "left")
    .join(fact_veh, "collision_id", "left")
    .withColumn("lesion", F.col("grave"))
    .withColumn("ksi", (F.coalesce("ksi", F.lit(0)) == 1) & F.col("grave"))
    .withColumn("ksi_amplia", (F.coalesce("ksi_amplia", F.lit(0)) == 1) & F.col("grave"))
    .withColumn("con_muerto", F.coalesce("muertos", F.lit(0)) > 0)
    .withColumn("causa", grupo_por_regex(F.col("factor_1"), GRUPOS_CAUSA, "Otra causa", SIN_CAUSA))
    .withColumn("alcohol_drogas", F.greatest(*[f.rlike(ALCOHOL).cast("int") for f in factores],
                                             F.coalesce("alcohol_veh", F.lit(0)), F.coalesce("alcohol_persona", F.lit(0))) == 1)
    .withColumn("velocidad", F.greatest(*[f.rlike(VELOCIDAD).cast("int") for f in factores],
                                        F.coalesce("velocidad_veh", F.lit(0))) == 1)
    .withColumn("franja", noche.col_franja(F.col("hora")))
    .withColumn("es_noche", noche.col_es_noche(F.col("hora")))
    .withColumn("fecha_noche", noche.col_fecha_noche(F.col("fecha"), F.col("hora")))
    .withColumn("dia_noche", mapa(DIAS_SEMANA)[F.dayofweek("fecha_noche")])
    .join(clima_hora, ["fecha", "hora"], "left")
    .join(cal_dia.withColumnRenamed("fecha", "fecha_noche"), "fecha_noche", "left")
    .fillna({"es_festivo": 0, "es_evento": 0, "n_vehiculos": 0, "hay_moto": 0, "hay_bici": 0,
             "hay_micromovilidad": 0, "hay_camion": 0, "hay_bus": 0, "hay_taxi": 0,
             "personas_ksi": 0, "peatones_ksi": 0, "ciclistas_micro_ksi": 0, "motociclistas_ksi": 0})
    .select("collision_id", "fecha", "anio", "mes", "hora", "franja", "es_noche", "fecha_noche", "dia_noche",
            "borough", "puma", "lat", "lon", "lesion", "ksi", "ksi_amplia", "con_muerto", "heridos", "muertos",
            "personas_ksi", "peatones_ksi", "ciclistas_micro_ksi", "motociclistas_ksi",
            "number_of_pedestrians_injured", "number_of_pedestrians_killed",
            "causa", "alcohol_drogas", "velocidad", "n_vehiculos", "hay_moto", "hay_bici", "hay_micromovilidad",
            "hay_camion", "hay_bus", "hay_taxi", "temperatura", "precipitacion_mm", "llueve", "nieva", "viento_kmh",
            "es_festivo", "es_evento", "nombre_fecha"))
ce.write.mode("overwrite").partitionBy("anio").parquet(f"{GOLD_FINAL}/choques_enriquecidos")
ce = spark.read.parquet(f"{GOLD_FINAL}/choques_enriquecidos")
n_ch, n_ce = choques.count(), ce.count()
assert n_ch == n_ce, f"La tabla enriquecida cambió el número de choques: {n_ch} vs {n_ce}"

resumen_ce = ce.agg(
    F.avg(F.col("hora").isNotNull().cast("int")).alias("con_hora"),
    F.avg(F.col("puma").isNotNull().cast("int")).alias("con_barrio"),
    F.avg(F.col("temperatura").isNotNull().cast("int")).alias("con_clima"),
    F.avg((F.col("causa") == SIN_CAUSA).cast("int")).alias("causa_sin_especificar"),
    F.sum(F.col("alcohol_drogas").cast("int")).alias("con_alcohol_o_drogas"),
    F.sum(F.col("velocidad").cast("int")).alias("con_velocidad")).toPandas()
print(resumen_ce.T)

anotar("Transformación", "choques", "Gravedad KSI por choque a partir de la tabla de personas",
       "Separar una muerte o lesión seria de un golpe leve (falla principal de la primera entrega)",
       efecto=f"{miles(ce.filter('ksi').count())} choques KSI de {miles(n_ce)}")
anotar("Transformación", "choques", "Causa agrupada (primer factor) y banderas de alcohol/drogas y velocidad con todos los factores",
       "Conectar cada choque con un tipo de control posible",
       efecto=f"{dec(100 * resumen_ce['causa_sin_especificar'][0])} % sin causa (categoría propia, no se imputa)")
anotar("Transformación", "choques", "Franja de 3 horas, marca de noche (21:00-05:59) y fecha de la noche",
       "Unidad temporal de la historia; una noche pertenece al día en que empieza", efecto="8 franjas, 3 nocturnas")
anotar("Transformación", "choques", "Unión del clima de la hora del choque y del calendario de la noche",
       "Medir el efecto de la lluvia, la nieve, los festivos y los eventos (P7)",
       efecto=f"{dec(100 * resumen_ce['con_clima'][0])} % de los choques con clima")

# %%
pe_out = (personas_e.join(ce.select("collision_id", "franja", "es_noche", "fecha_noche", "dia_noche", "puma", "borough",
                                    "causa", "alcohol_drogas", "velocidad", "llueve"), "collision_id", "left")
          .select("unique_id", "collision_id", "fecha", "anio", "hora", "franja", "es_noche", "fecha_noche", "dia_noche",
                  "puma", "borough", "tipo_persona", "tipo_usuario", "grupo_vehiculo", "muerto", "lesionado", "ksi",
                  "ksi_amplia", "lesion_sin_detalle", "edad", "sexo", "causa", "alcohol_drogas", "velocidad", "llueve"))
pe_out.write.mode("overwrite").partitionBy("anio").parquet(f"{GOLD_FINAL}/personas_enriquecidas")
print("Personas por tipo de usuario:")
spark.read.parquet(f"{GOLD_FINAL}/personas_enriquecidas").groupBy("tipo_usuario").agg(
    F.count("*").alias("personas"), F.sum(F.col("ksi").cast("int")).alias("ksi")).orderBy(F.desc("ksi")).show()

# %% [markdown]
# ## 4. Filtros para el panel nocturno
#
# El panel necesita hora y barrio. Medimos cuántos choques se pierden y si los KSI se pierden más que el resto, porque eso sesgaría la gravedad.

# %%
def contar(df):
    r = df.agg(F.count("*").alias("n"), F.sum(F.col("ksi").cast("int")).alias("ksi")).first()
    return r["n"], r["ksi"] or 0

base_n, base_k = contar(ce)
c1_n, c1_k = contar(ce.filter(F.col("hora").isNotNull()))
anotar("Filtro", "panel", "Choques con hora registrada", "Sin hora no se puede asignar a una franja", base_n, c1_n)
c2 = ce.filter(F.col("hora").isNotNull() & F.col("puma").isNotNull())
c2_n, c2_k = contar(c2)
anotar("Filtro", "panel", "Choques con barrio (coordenadas dentro de una PUMA)", "Sin lugar no se puede asignar a un barrio", c1_n, c2_n)
print(f"Choques KSI conservados: {100 * c2_k / base_k:.1f} % frente a {100 * c2_n / base_n:.1f} % de todos los choques")
c3 = c2.filter("es_noche")
c3_n, c3_k = contar(c3)
anotar("Filtro", "panel", "Choques nocturnos (21:00 a 05:59)", "El eje de la historia es la noche", c2_n, c3_n)

# %% [markdown]
# ## 5. Panel barrio × noche × franja nocturna
#
# La grilla tiene las 55 PUMA, todas las noches desde 2019 y las 3 franjas nocturnas. Las celdas sin choques quedan en cero. Si las quitáramos, el modelo solo vería celdas con choques y aprendería un riesgo inflado.

# %%
pumas_df = poblacion.select("puma", "borough", "poblacion_2020", "densidad_km2")
ultima_noche = F.date_sub(F.lit(corte), 1)          # la noche del día de corte termina fuera del periodo
noches = (spark.range(1).select(F.explode(F.sequence(F.to_date(F.lit(FECHA_INICIO)), ultima_noche)).alias("fecha_noche")))
franjas = spark.createDataFrame([(f, i) for i, f in enumerate(FRANJAS_NOCHE)], "franja int, orden_franja int")
grilla = pumas_df.crossJoin(noches).crossJoin(franjas)

conteos = c3.groupBy("puma", "fecha_noche", "franja").agg(
    F.count("*").alias("choques"),
    F.sum(F.col("lesion").cast("int")).alias("con_lesionados"),
    F.sum(F.col("ksi").cast("int")).alias("choques_ksi"),
    F.sum(F.col("ksi_amplia").cast("int")).alias("choques_ksi_amplia"),
    F.sum("personas_ksi").alias("personas_ksi"),
    F.sum(F.coalesce("muertos", F.lit(0))).alias("muertos"),
    F.sum((F.col("lesion") & (F.col("alcohol_drogas") | F.col("velocidad"))).cast("int")).alias("lesion_alcohol_velocidad"))

clima_franja = (clima.filter(F.col("franja").isin(FRANJAS_NOCHE)).groupBy("fecha_noche", "franja").agg(
    # si a la franja le falta una hora, su lluvia y su nieve quedan vacías: sin dato no es seco
    F.when(F.count("precipitacion_mm") == F.count("*"), F.sum("precipitacion_mm")).alias("precipitacion_mm"),
    F.when(F.count("llueve") == F.count("*"), F.max(F.col("llueve").cast("int"))).alias("llueve"),
    F.when(F.count("nieva") == F.count("*"), F.max(F.col("nieva").cast("int"))).alias("nieva"),
    F.avg("temperatura").alias("temperatura"),
    F.avg("viento_kmh").alias("viento_kmh"),
    F.count("temperatura").alias("horas_con_clima")))

arr = (arrestos.filter(F.col("puma").isNotNull())
       .withColumn("dwi", noche.col_arresto_dwi(F.col("law_code")))
       .withColumn("transito", noche.col_arresto_transito(F.col("law_code")))
       .groupBy("puma", F.col("fecha").alias("fecha_noche")).agg(
           F.sum(F.col("dwi").cast("int")).alias("arrestos_dwi"),
           F.sum(F.col("transito").cast("int")).alias("arrestos_transito")))

panel = (grilla
    .join(conteos, ["puma", "fecha_noche", "franja"], "left")
    .join(clima_franja, ["fecha_noche", "franja"], "left")
    .join(cal_dia.withColumnRenamed("fecha", "fecha_noche"), "fecha_noche", "left")
    .fillna(0, subset=["choques", "con_lesionados", "choques_ksi", "choques_ksi_amplia", "personas_ksi", "muertos",
                       "lesion_alcohol_velocidad", "es_festivo", "es_evento"])
    .withColumn("noche_idx", F.datediff("fecha_noche", F.to_date(F.lit(FECHA_INICIO))))
    .withColumn("anio", F.year("fecha_noche"))
    .withColumn("mes", F.month("fecha_noche"))
    .withColumn("dia_noche", mapa(DIAS_SEMANA)[F.dayofweek("fecha_noche")])
    .withColumn("fin_de_semana", F.col("dia_noche").isin("Viernes", "Sábado").cast("int"))
    .persist())

# %% [markdown]
# ### Variables de historia reciente
#
# Para cada celda contamos los choques del mismo barrio en las 28 y las 364 noches anteriores, y los arrestos por embriaguez y de tránsito de los días anteriores. La ventana termina una noche antes, así que ninguna variable usa información de la noche que se quiere predecir.

# %%
por_noche = (panel.groupBy("puma", "noche_idx").agg(
    F.sum("choques_ksi").alias("ksi_n"), F.sum("con_lesionados").alias("lesion_n"),
    F.sum("lesion_alcohol_velocidad").alias("alcvel_n"))
    .join(arr.withColumn("noche_idx", F.datediff("fecha_noche", F.to_date(F.lit(FECHA_INICIO)))).drop("fecha_noche"),
          ["puma", "noche_idx"], "left")
    .fillna(0, subset=["arrestos_dwi", "arrestos_transito"]))

rezagos = por_noche
for v in VENTANAS_REZAGO:
    w = Window.partitionBy("puma").orderBy("noche_idx").rangeBetween(-v, -1)
    rezagos = (rezagos
        .withColumn(f"ksi_previos_{v}", F.sum("ksi_n").over(w))
        .withColumn(f"lesion_previos_{v}", F.sum("lesion_n").over(w))
        .withColumn(f"alcvel_previos_{v}", F.sum("alcvel_n").over(w))
        .withColumn(f"dwi_previos_{v}", F.sum("arrestos_dwi").over(w))
        .withColumn(f"transito_previos_{v}", F.sum("arrestos_transito").over(w)))
rezagos = rezagos.fillna(0).drop("ksi_n", "lesion_n", "alcvel_n")

v_largo = max(VENTANAS_REZAGO)
w_franja = Window.partitionBy("puma", "franja").orderBy("noche_idx").rangeBetween(-v_largo, -1)
panel = (panel.join(rezagos, ["puma", "noche_idx"], "left")
    .withColumn(f"ksi_franja_previos_{v_largo}", F.coalesce(F.sum("choques_ksi").over(w_franja), F.lit(0)))
    .withColumn(f"lesion_franja_previos_{v_largo}", F.coalesce(F.sum("con_lesionados").over(w_franja), F.lit(0)))
    .withColumn("historia_completa", F.col("noche_idx") >= v_largo)
    .withColumn("obj_ksi", (F.col("choques_ksi") > 0).cast("int"))
    .withColumn("obj_lesion", (F.col("con_lesionados") > 0).cast("int")))

panel.write.mode("overwrite").partitionBy("anio").parquet(f"{GOLD_FINAL}/panel_noche")
panel = spark.read.parquet(f"{GOLD_FINAL}/panel_noche")
n_celdas = panel.count()
esperadas = 55 * noches.count() * len(FRANJAS_NOCHE)
assert n_celdas == esperadas, f"El panel tiene {n_celdas} celdas y se esperaban {esperadas}"

# Control: los totales del panel cuadran con el conteo directo de choques nocturnos con barrio
tot = panel.agg(F.sum("choques").alias("choques"), F.sum("choques_ksi").alias("ksi")).first()
directo = c3.filter(F.col("fecha_noche").between(F.to_date(F.lit(FECHA_INICIO)), ultima_noche)).agg(F.count("*").alias("n"), F.sum(F.col("ksi").cast("int")).alias("k")).first()
assert tot["choques"] == directo["n"] and tot["ksi"] == directo["k"], "Los totales del panel no cuadran con la plata"
print(f"Panel: {n_celdas:,} celdas (55 barrios × noches × 3 franjas). Totales verificados contra la plata.")

anotar("Transformación", "panel", "Grilla completa barrio × noche × franja nocturna con ceros reales",
       "Base del modelo predictivo; una celda sin choques es información", efecto=f"{miles(n_celdas)} celdas")
anotar("Transformación", "panel", f"Historia reciente por celda: choques y arrestos de las {VENTANAS_REZAGO} noches previas",
       "Dar al modelo la memoria que usaría un analista sin mirar el futuro", efecto=f"{5 * len(VENTANAS_REZAGO) + 2} variables")

prevalencia = panel.filter(F.col("fecha_noche") >= MODELO_INICIO).agg(
    F.avg("obj_ksi").alias("celdas_con_ksi"), F.avg("obj_lesion").alias("celdas_con_lesionados")).toPandas()
print("Prevalencia desde", MODELO_INICIO, ":\n", (100 * prevalencia).round(2).T)

# %% [markdown]
# ## 6. Barrios: riesgo nocturno, control y brecha
#
# Usamos 2021 a 2025, los años completos con las mismas reglas de reporte. Las tasas son anuales, por 100.000 habitantes del censo 2020.
#
# El riesgo son los choques KSI nocturnos por 100.000 habitantes al año. El control son los arrestos por embriaguez (VTL 1192) con la misma base, contando todos los del barrio porque no traen hora.
#
# La brecha es el percentil de riesgo menos el percentil de control. Va de −1 a 1, y cuando es alta el barrio tiene mucho riesgo y poco control.

# %%
ANIOS_BRECHA = [a for a in ANIOS_COMPLETOS if a >= 2021]
n_anios = len(ANIOS_BRECHA)
pb = panel.filter(F.col("anio").isin(ANIOS_BRECHA))
riesgo = pb.groupBy("puma").agg(
    F.sum("choques_ksi").alias("ksi_noche"), F.sum("con_lesionados").alias("lesion_noche"),
    F.sum("lesion_alcohol_velocidad").alias("lesion_alcvel_noche"),
    # sensibilidad a la lluvia: choques con lesionados por celda con lluvia frente a sin lluvia
    F.avg(F.when(F.col("llueve") == 1, F.col("con_lesionados"))).alias("lesion_por_celda_lluvia"),
    F.avg(F.when(F.col("llueve") == 0, F.col("con_lesionados"))).alias("lesion_por_celda_seca"))
ksi_dia = (ce.filter(F.col("anio").isin(ANIOS_BRECHA) & F.col("puma").isNotNull() & F.col("hora").isNotNull())
           .groupBy("puma").agg(F.sum(F.col("ksi").cast("int")).alias("ksi_todas_horas")))
victimas = (spark.read.parquet(f"{GOLD_FINAL}/personas_enriquecidas")
            .filter(F.col("anio").isin(ANIOS_BRECHA) & F.col("es_noche") & F.col("ksi") & F.col("puma").isNotNull())
            .groupBy("puma").agg(F.count("*").alias("personas_ksi_noche"),
                                 F.sum((F.col("tipo_usuario") == "Peatón").cast("int")).alias("peatones_ksi_noche")))
control = (arrestos.filter(F.col("anio").isin(ANIOS_BRECHA) & F.col("puma").isNotNull())
           .groupBy("puma").agg(F.sum(noche.col_arresto_dwi(F.col("law_code")).cast("int")).alias("arrestos_dwi"),
                                F.sum(noche.col_arresto_transito(F.col("law_code")).cast("int")).alias("arrestos_transito")))

barrios = (poblacion.join(riesgo, "puma", "left").join(ksi_dia, "puma", "left").join(victimas, "puma", "left")
           .join(control, "puma", "left").toPandas().fillna({"personas_ksi_noche": 0, "peatones_ksi_noche": 0}))

por100k = lambda col: barrios[col] / n_anios / barrios["poblacion_2020"] * 1e5
barrios["tasa_ksi_noche"] = por100k("ksi_noche")
barrios["tasa_lesion_noche"] = por100k("lesion_noche")
barrios["tasa_dwi"] = por100k("arrestos_dwi")
barrios["tasa_transito"] = por100k("arrestos_transito")
barrios["pct_alcohol_velocidad"] = 100 * barrios["lesion_alcvel_noche"] / barrios["lesion_noche"]
barrios["pct_ksi_de_noche"] = 100 * barrios["ksi_noche"] / barrios["ksi_todas_horas"]
barrios["pct_peatones_ksi_noche"] = 100 * barrios["peatones_ksi_noche"] / barrios["personas_ksi_noche"].replace(0, np.nan)
barrios["sensibilidad_lluvia"] = barrios["lesion_por_celda_lluvia"] / barrios["lesion_por_celda_seca"]

barrios = barrios.merge(noche.nombres_puma()[["puma", "barrio"]], on="puma", how="left")
assert barrios["barrio"].notna().all(), "Hay barrios sin nombre"
barrios["percentil_riesgo"] = noche.percentil(barrios["tasa_ksi_noche"])
barrios["percentil_control"] = noche.percentil(barrios["tasa_dwi"])
barrios["brecha"] = barrios["percentil_riesgo"] - barrios["percentil_control"]
# Como comprobación calculamos la brecha de otra forma, con el residuo de log(control) sobre log(riesgo). Si es negativo hay menos control del esperado.
x = np.log(barrios["tasa_ksi_noche"] + 0.1)
y = np.log(barrios["tasa_dwi"] + 0.1)
pendiente, intercepto = np.polyfit(x, y, 1)
barrios["brecha_residuo"] = -(y - (intercepto + pendiente * x))
print(f"Spearman entre las dos medidas de brecha: {noche.spearman(barrios['brecha'], barrios['brecha_residuo']):.2f}")

spark.createDataFrame(barrios).write.mode("overwrite").parquet(f"{GOLD_FINAL}/barrios_noche")
barrios.round(3).to_csv("../figuras/tabla_09_barrios_noche.csv", index=False)
anotar("Transformación", "barrios", "Tasas por 100.000 habitantes con la población del censo 2020",
       "Comparar barrios de tamaño distinto", efecto=f"55 barrios, {n_anios} años (2021-2025)")
anotar("Transformación", "barrios", "Índice de brecha de control (percentil de riesgo − percentil de control)",
       "Resumir en un número el desajuste entre riesgo y control", efecto="de −1 a 1; residuo como comprobación")
barrios.sort_values("brecha", ascending=False)[["puma", "barrio", "borough", "poblacion_2020", "tasa_ksi_noche", "tasa_dwi", "brecha"]].head(10).round(2)

# %% [markdown]
# ## 7. Registro de filtros y transformaciones (numeral 9)

# %%
reg = pd.DataFrame(registro)
reg.to_csv("../figuras/tabla_09_filtros_transformaciones.csv", index=False)
print(f"Tiempo total: {(time.time() - t_inicio) / 60:.1f} min")
reg
