# %% [markdown]
# # 10 - Respuestas a las preguntas de negocio
#
# Al volver a la fase de entendimiento de CRISP-DM reformulamos las ocho preguntas de la primera entrega. Las nuevas siguen con los dos indicadores del caso (arrestos y accidentes viales) y apuntan a decisiones que la ciudad puede tomar. La última columna dice de cuál pregunta original viene cada una.
#
# | N.º | Pregunta | Indicador | Viene de |
# |---|---|---|---|
# | P1 | ¿Cómo cambiaron entre 2019 y 2025 los arrestos totales y los arrestos por conducir bajo efectos del alcohol o las drogas en cada distrito? | Arrestos | 1 |
# | P2 | ¿Qué parte de los arrestos es por delitos menores, cuáles son y en qué distritos, según su pobreza, hay más margen para usar alternativas al arresto? | Arrestos | 2 y 3 |
# | P3 | ¿Qué parte de las muertes y lesiones serias en las vías ocurre de noche y en qué horas y días se concentra? | Accidentes | 6 |
# | P4 | ¿Qué causas y qué usuarios de la vía están detrás de los choques nocturnos con muertos o lesionados serios? | Accidentes | 5 |
# | P5 | ¿Qué barrios tienen el mayor riesgo nocturno y se mantiene ese orden de un año a otro? | Accidentes | 4 |
# | P6 | ¿Están los arrestos por embriaguez al volante en los barrios con mayor riesgo nocturno? | Ambos | 8 |
# | P7 | ¿Cómo cambia el riesgo nocturno con la lluvia, la temperatura, los festivos y los fines de semana? | Accidentes | 6 |
# | P8 | ¿Qué barrios combinan muchos arrestos y muchos choques graves, y qué intervención le conviene a cada uno? | Ambos | 7 |
#
# La pregunta P9, sobre si un modelo puede anticipar dónde poner el control cada noche, se responde en el notebook 11.
#
# Llamamos KSI al choque que deja al menos un muerto o un lesionado serio. Hay que tener en cuenta que los arrestos miden la actividad de la policía, por lo que no sirven para medir la criminalidad. Además, las tasas por residente inflan las zonas con mucha población flotante, y las relaciones que se muestran no son causales.

# %%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm
from matplotlib.ticker import FuncFormatter, PercentFormatter
from pyspark.sql import functions as F

import geo
import noche
from codigos import ORDEN_DIAS, SIN_CAUSA
from config import GOLD_FINAL, SILVER, SILVER_FINAL, sesion
from decisiones import ANIOS_COMPLETOS, HORA_FIN_NOCHE, HORA_INICIO_NOCHE, MINIMO_CASOS_RANKING
from estilo import BOROUGHS, CATEGORICA, COLOR_BOROUGH, SECUENCIAL, TINTA_SECUNDARIA, dec, guardar, miles

spark = sesion("10_respuestas")
ACENTO, GRIS = CATEGORICA[0], "#b8b7b1"
ANIOS_RECIENTES = [a for a in ANIOS_COMPLETOS if a >= 2021]      # mismo régimen de reporte
N_RECIENTES = len(ANIOS_RECIENTES)

arrestos = spark.read.parquet(f"{SILVER}/arrestos")
pobreza = spark.read.parquet(f"{SILVER}/pobreza")
ce = spark.read.parquet(f"{GOLD_FINAL}/choques_enriquecidos").cache()
pe = spark.read.parquet(f"{GOLD_FINAL}/personas_enriquecidas")
panel = spark.read.parquet(f"{GOLD_FINAL}/panel_noche")
barrios = spark.read.parquet(f"{GOLD_FINAL}/barrios_noche").toPandas().set_index("puma").sort_index()
pob_borough = (spark.read.parquet(f"{SILVER_FINAL}/poblacion_puma_2020").toPandas()
               .groupby("borough")["poblacion_2020"].sum().reindex(BOROUGHS))
pumas = geo.cargar_pumas(spark)

def colores_por_valor(serie, n=6):
    """Un color por sextil del valor (para los mapas)."""
    cortes = pd.qcut(serie.rank(method="first"), n, labels=False)
    return {k: SECUENCIAL[int(c)] for k, c in cortes.items()}

def tabla(df, nombre):
    df.to_csv(f"../figuras/tabla_10_{nombre}.csv")
    return df

def relacion(r):
    """Cómo se describe una correlación de Spearman."""
    fuerza = "muy débil" if abs(r) < 0.2 else "débil" if abs(r) < 0.4 else "moderada" if abs(r) < 0.6 else "fuerte"
    return fuerza if abs(r) < 0.2 else f"{fuerza} y {'positiva' if r > 0 else 'negativa'}"

def cambio(x):
    return f"{'subieron' if x >= 0 else 'bajaron'} {dec(abs(x), 0)} %"

# %% [markdown]
# ## P1. ¿Cómo cambiaron los arrestos totales y los de embriaguez al volante?
#
# Las tasas son por 100.000 habitantes de 2020. Como de 2026 solo hay un semestre, va en la tabla como conteo y no se grafica.

# %%
p1 = (arrestos.filter(F.col("borough").isNotNull())
      .withColumn("dwi", noche.col_arresto_dwi(F.col("law_code")))
      .groupBy("borough", "anio").agg(F.count("*").alias("arrestos"), F.sum(F.col("dwi").cast("int")).alias("arrestos_dwi"))
      .toPandas().sort_values(["borough", "anio"]))
p1["poblacion_2020"] = p1["borough"].map(pob_borough)
completo = p1["anio"].isin(ANIOS_COMPLETOS)
p1["tasa_arrestos"] = (1e5 * p1["arrestos"] / p1["poblacion_2020"]).where(completo)
p1["tasa_dwi"] = (1e5 * p1["arrestos_dwi"] / p1["poblacion_2020"]).where(completo)
tabla(p1.set_index(["borough", "anio"]).round(1), "p1_arrestos_distrito_anio")

ciudad = p1[completo].groupby("anio")[["arrestos", "arrestos_dwi"]].sum()
cambio_total = 100 * (ciudad.loc[2025, "arrestos"] / ciudad.loc[2019, "arrestos"] - 1)
cambio_dwi = 100 * (ciudad.loc[2025, "arrestos_dwi"] / ciudad.loc[2019, "arrestos_dwi"] - 1)

fig, axes = plt.subplots(1, 2, figsize=(12, 4))
for ax, columna, titulo in [(axes[0], "tasa_arrestos", "Todos los arrestos"), (axes[1], "tasa_dwi", "Embriaguez al volante (VTL 1192)")]:
    for b in BOROUGHS:
        d = p1[(p1["borough"] == b) & completo]
        ax.plot(d["anio"], d[columna], color=COLOR_BOROUGH[b], marker="o", markersize=3, label=b)
    ax.yaxis.set_major_formatter(FuncFormatter(miles))
    ax.set_title(titulo, fontsize=10)
axes[0].set_ylabel("arrestos por 100.000 hab.")
axes[1].legend(fontsize=8)
fig.suptitle(f"Entre 2019 y 2025 los arrestos {cambio(cambio_total)} y los de embriaguez al volante {cambio(cambio_dwi)}",
             x=0.01, ha="left", fontsize=11, fontweight="bold")
fig.tight_layout()
guardar(fig, "10_p1_arrestos_evolucion")
plt.show()

# %% [markdown]
# ## P2. ¿Dónde hay margen para alternativas al arresto?
#
# En los delitos menores están los candidatos a citaciones y programas de desvío. Con cinco distritos no alcanza para calcular una correlación con la pobreza, así que los ordenamos y los comparamos.

# %%
pob = (pobreza.groupBy("borough").agg((F.sum(F.when(F.col("en_pobreza"), F.col("peso_persona")).otherwise(0)) /
                                       F.sum("peso_persona") * 100).alias("pobreza_pct")).toPandas().set_index("borough"))
completos = arrestos.filter(F.col("anio").isin(ANIOS_COMPLETOS) & F.col("borough").isNotNull())
grav = (completos.groupBy("borough", "gravedad").count().toPandas()
        .pivot(index="borough", columns="gravedad", values="count").reindex(BOROUGHS).fillna(0))
por_anio = 1e5 / len(ANIOS_COMPLETOS) / pob_borough
p2 = pd.DataFrame({"pobreza_pct_2018": pob["pobreza_pct"].reindex(BOROUGHS),
                   "arrestos_por_100k": grav.sum(axis=1) * por_anio,
                   "felonias_por_100k": grav["Felonía"] * por_anio,
                   "menores_por_100k": grav["Delito menor"] * por_anio,
                   "pct_delitos_menores": 100 * grav["Delito menor"] / grav.sum(axis=1)}).sort_values("pobreza_pct_2018", ascending=False)
tabla(p2.round(1), "p2_arrestos_pobreza")
menores = (completos.filter((F.col("gravedad") == "Delito menor") & F.col("ofns_desc").isNotNull())
           .groupBy("ofns_desc").count().orderBy(F.desc("count")).toPandas().set_index("ofns_desc"))
menores["pct_de_los_menores"] = 100 * menores["count"] / menores["count"].sum()
tabla(menores.head(15).round(2), "p2_tipos_delito_menor")
pct_menores = 100 * grav["Delito menor"].sum() / grav.values.sum()

fig, axes = plt.subplots(1, 2, figsize=(13, 4.4), gridspec_kw={"width_ratios": [1, 1.1]})
ax = axes[0]
x = np.arange(len(p2))
ax.bar(x - 0.18, p2["felonias_por_100k"], width=0.36, color=GRIS, label="Felonías")
ax.bar(x + 0.18, p2["menores_por_100k"], width=0.36, color=ACENTO, label="Delitos menores")
ax.set_xticks(x)
ax.set_xticklabels([f"{b}\npobreza {dec(p)} %" for b, p in p2["pobreza_pct_2018"].items()], fontsize=8)
ax.yaxis.set_major_formatter(FuncFormatter(miles))
ax.set_ylabel("arrestos por 100.000 hab. al año")
ax.legend()
ax.grid(axis="x", visible=False)
ax = axes[1]
m8 = menores.head(8).iloc[::-1]
ax.barh(m8.index.str.capitalize().str.slice(0, 42), m8["pct_de_los_menores"], color=ACENTO)
ax.xaxis.set_major_formatter(PercentFormatter(decimals=0))
ax.set_title("Delitos menores más frecuentes", fontsize=10)
ax.grid(axis="y", visible=False)
fig.suptitle(f"El {dec(pct_menores)} % de los arrestos es por delitos menores; la tasa más alta es la de {p2['menores_por_100k'].idxmax()}",
             x=0.01, ha="left", fontsize=11, fontweight="bold")
fig.tight_layout()
guardar(fig, "10_p2_margen_alternativas")
plt.show()

# %% [markdown]
# ## P3. ¿Qué parte de las muertes y lesiones serias ocurre de noche?
#
# La noche va de 21:00 a 05:59, 9 de las 24 horas.

# %%
con_hora = ce.filter(F.col("hora").isNotNull())
p3 = (con_hora.groupBy("anio", "es_noche").agg(
    F.count("*").alias("choques"), F.sum(F.col("lesion").cast("int")).alias("con_lesionados"),
    F.sum(F.col("ksi").cast("int")).alias("ksi"), F.sum(F.coalesce("muertos", F.lit(0))).alias("muertos"))
    .toPandas())
medidas = ["choques", "con_lesionados", "ksi", "muertos"]
tot = p3.groupby("anio")[medidas].transform("sum")
for c in medidas:
    p3[f"pct_{c}"] = 100 * p3[c] / tot[c]
p3["ksi_por_100_lesion"] = 100 * p3["ksi"] / p3["con_lesionados"]
p3 = p3.sort_values(["anio", "es_noche"])
tabla(p3.set_index(["anio", "es_noche"]).round(2), "p3_noche_por_anio")

rec = p3[p3["anio"].isin(ANIOS_RECIENTES)].groupby("es_noche")[medidas].sum()
partes = 100 * rec.loc[True] / rec.sum()
tabla(partes.round(2).to_frame("pct_de_noche"), "p3_participacion_noche")

fig, axes = plt.subplots(1, 2, figsize=(12, 4), gridspec_kw={"width_ratios": [1, 1.3]})
ax = axes[0]
etiquetas = ["Muertos", "KSI", "Con lesionados", "Choques"]
valores = partes[medidas[::-1]].values
ax.barh(etiquetas, valores, color=[ACENTO, ACENTO, GRIS, GRIS])
for i, v in enumerate(valores):
    ax.text(v, i, f" {dec(v, 0)} %", va="center", fontsize=9)
ax.axvline(100 * 9 / 24, color=TINTA_SECUNDARIA, linestyle=":", linewidth=1)
ax.xaxis.set_major_formatter(PercentFormatter(decimals=0))
ax.set_title("Parte que ocurre de noche, 2021-2025 (línea: 9 de 24 horas)", fontsize=10)
ax.grid(axis="y", visible=False)
ax = axes[1]
for es_noche, color, nombre in [(True, ACENTO, "Noche"), (False, GRIS, "Día")]:
    d = p3[(p3["es_noche"] == es_noche) & p3["anio"].isin(ANIOS_COMPLETOS)]
    ax.plot(d["anio"], d["ksi_por_100_lesion"], color=color, marker="o", markersize=4, label=nombre)
ax.legend()
ax.set_ylabel("KSI por cada 100 choques\ncon lesionados")
ax.set_title("Gravedad por año", fontsize=10)
nexo = "pero" if partes["muertos"] > partes["choques"] else "y"
fig.suptitle(f"De noche ocurre el {dec(partes['choques'], 0)} % de los choques {nexo} el {dec(partes['muertos'], 0)} % de las muertes",
             x=0.01, ha="left", fontsize=12, fontweight="bold")
fig.tight_layout()
guardar(fig, "10_p3_noche_grave")
plt.show()

# %%
hd = (ce.filter(F.col("anio").isin(ANIOS_RECIENTES) & F.col("ksi") & F.col("hora").isNotNull())
      .withColumn("dia", (F.dayofweek("fecha") + 5) % 7 + 1)          # 1 = lunes ... 7 = domingo
      .groupBy("dia", "hora").count().toPandas()
      .pivot(index="dia", columns="hora", values="count").reindex(index=range(1, 8), columns=range(24)).fillna(0))
hd.index = ORDEN_DIAS
tabla(hd, "p3_ksi_hora_dia")
dia_max, hora_max = hd.stack().idxmax()

fig, ax = plt.subplots(figsize=(11, 3.6))
im = ax.imshow(hd.values, aspect="auto", cmap=LinearSegmentedColormap.from_list("azules", SECUENCIAL))
ax.set_xticks(range(24))
ax.set_yticks(range(7))
ax.set_yticklabels(ORDEN_DIAS, fontsize=8)
for borde in (HORA_FIN_NOCHE + 0.5, HORA_INICIO_NOCHE - 0.5):
    ax.axvline(borde, color="white", linewidth=1.5)
ax.set_xlabel("hora del día (las líneas separan la noche)")
ax.grid(False)
fig.colorbar(im, ax=ax, fraction=0.02, pad=0.01, label="choques KSI")
ax.set_title(f"El momento con más choques KSI es el {dia_max.lower()} a las {int(hora_max):02d}:00 (2021-2025)", pad=12, fontsize=11)
guardar(fig, "10_p3_ksi_hora_dia")
plt.show()

# %% [markdown]
# ## P4. ¿Qué causas y qué usuarios están detrás de la gravedad nocturna?
#
# El denominador son los choques nocturnos con lesionados de 2021 a 2025. En el ranking solo entran las causas con al menos `MINIMO_CASOS_RANKING` casos, aunque la tabla las trae todas. Los choques sin causa quedan como categoría aparte, de modo que lo que medimos de alcohol es un piso.

# %%
lesion = ce.filter(F.col("anio").isin(ANIOS_RECIENTES) & F.col("lesion") & F.col("hora").isNotNull())
p4 = (lesion.filter("es_noche").groupBy("causa")
      .agg(F.count("*").alias("con_lesionados"), F.sum(F.col("ksi").cast("int")).alias("ksi")).toPandas())
p4["ksi_por_100"] = 100 * p4["ksi"] / p4["con_lesionados"]
p4["ic_bajo"], p4["ic_alto"] = [100 * v for v in noche.wilson(p4["ksi"], p4["con_lesionados"])]
p4["pct_de_los_ksi"] = 100 * p4["ksi"] / p4["ksi"].sum()
p4 = p4.sort_values("ksi_por_100")
tabla(p4.set_index("causa").round(2), "p4_causas_noche")

banderas = {"hay_moto": "Motocicleta", "hay_bici": "Bicicleta", "hay_micromovilidad": "Micromovilidad eléctrica",
            "hay_camion": "Camión", "hay_bus": "Bus", "hay_taxi": "Taxi"}
r = lesion.filter("es_noche").agg(*[F.sum(F.col(c)).alias(f"{c}_n") for c in banderas],
                                  *[F.sum(F.col(c) * F.col("ksi").cast("int")).alias(f"{c}_k") for c in banderas]).first()
p4v = pd.DataFrame([{"vehiculo": nombre, "con_lesionados": r[f"{c}_n"], "ksi": r[f"{c}_k"]} for c, nombre in banderas.items()])
p4v["ksi_por_100"] = 100 * p4v["ksi"] / p4v["con_lesionados"]
tabla(p4v.set_index("vehiculo").round(2), "p4_vehiculos_noche")
base_p4 = 100 * p4["ksi"].sum() / p4["con_lesionados"].sum()

rk = p4[(p4["con_lesionados"] >= MINIMO_CASOS_RANKING) & (p4["causa"] != SIN_CAUSA)]
fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.4), gridspec_kw={"width_ratios": [1.4, 1]})
ax = axes[0]
ax.barh(rk["causa"], rk["ksi_por_100"], xerr=[rk["ksi_por_100"] - rk["ic_bajo"], rk["ic_alto"] - rk["ksi_por_100"]],
        color=np.where(rk["causa"].isin(["Alcohol o drogas", "Exceso de velocidad o agresividad"]), ACENTO, GRIS),
        error_kw={"elinewidth": 0.8, "ecolor": TINTA_SECUNDARIA})
ax.set_title("Por causa", fontsize=10)
vv = p4v.sort_values("ksi_por_100")
axes[1].barh(vv["vehiculo"], vv["ksi_por_100"], color=GRIS)
axes[1].set_title("Cuando participa cada vehículo", fontsize=10)
for ax in axes:
    ax.axvline(base_p4, color=TINTA_SECUNDARIA, linestyle="--", linewidth=1)
    ax.set_xlabel("KSI por cada 100 choques nocturnos con lesionados")
    ax.grid(axis="y", visible=False)
top = rk.iloc[-1]
fig.suptitle(f"De noche, la causa más grave es «{top['causa']}»: {dec(top['ksi_por_100'])} KSI por cada 100 choques con lesionados "
             f"(promedio {dec(base_p4)})", x=0.01, ha="left", fontsize=11, fontweight="bold")
fig.tight_layout()
guardar(fig, "10_p4_causas_vehiculos")
plt.show()

# %%
factores = (lesion.groupBy("es_noche").agg(
    F.count("*").alias("con_lesionados"),
    F.avg(F.col("alcohol_drogas").cast("int")).alias("alcohol_drogas"),
    F.avg(F.col("velocidad").cast("int")).alias("velocidad"),
    F.avg((F.col("causa") == SIN_CAUSA).cast("int")).alias("sin_causa"),
    F.sum(F.col("ksi").cast("int")).alias("ksi"),
    F.sum((F.col("ksi") & (F.col("alcohol_drogas") | F.col("velocidad"))).cast("int")).alias("ksi_alcvel"))
    .toPandas().set_index("es_noche"))
factores["pct_ksi_con_alcohol_o_velocidad"] = 100 * factores["ksi_alcvel"] / factores["ksi"]
tabla(factores.round(4), "p4_factores_noche_dia")

usuarios = (pe.filter(F.col("anio").isin(ANIOS_RECIENTES) & F.col("ksi") & F.col("es_noche").isNotNull())
            .groupBy("es_noche", "tipo_usuario").count().toPandas()
            .pivot(index="tipo_usuario", columns="es_noche", values="count").fillna(0))
usuarios = 100 * usuarios / usuarios.sum()
usuarios.columns = ["Noche" if c else "Día" for c in usuarios.columns]
usuarios = usuarios.sort_values("Noche")
tabla(usuarios.round(1), "p4_victimas_ksi_usuario")

fig, axes = plt.subplots(1, 2, figsize=(12, 4))
ax = axes[0]
x = np.arange(2)
dia = [100 * float(factores.loc[False, c]) for c in ("alcohol_drogas", "velocidad")]
de_noche = [100 * float(factores.loc[True, c]) for c in ("alcohol_drogas", "velocidad")]
ax.bar(x - 0.18, dia, width=0.36, color=GRIS, label="Día")
ax.bar(x + 0.18, de_noche, width=0.36, color=ACENTO, label="Noche")
ax.set_xticks(x)
ax.set_xticklabels(["Alcohol o drogas", "Velocidad o agresividad"])
ax.yaxis.set_major_formatter(PercentFormatter(decimals=0))
ax.set_title("% de los choques con lesionados con cada factor", fontsize=10)
ax.legend()
ax.grid(axis="x", visible=False)
ax = axes[1]
y = np.arange(len(usuarios))
ax.barh(y - 0.18, usuarios["Día"], height=0.36, color=GRIS, label="Día")
ax.barh(y + 0.18, usuarios["Noche"], height=0.36, color=ACENTO, label="Noche")
ax.set_yticks(y)
ax.set_yticklabels(usuarios.index)
ax.xaxis.set_major_formatter(PercentFormatter(decimals=0))
ax.set_title("Quiénes son las víctimas KSI", fontsize=10)
ax.grid(axis="y", visible=False)
fig.suptitle(f"El alcohol o las drogas aparecen en el {dec(de_noche[0])} % de los choques con lesionados de noche y en el "
             f"{dec(dia[0])} % de día", x=0.01, ha="left", fontsize=11, fontweight="bold")
fig.tight_layout()
guardar(fig, "10_p4_factores_usuarios")
plt.show()

# %% [markdown]
# ## P5. ¿Qué barrios tienen el mayor riesgo nocturno y es estable?
#
# Choques KSI nocturnos por 100.000 habitantes al año, de 2021 a 2025.

# %%
anual = (panel.filter(F.col("anio").isin(ANIOS_RECIENTES)).groupBy("puma", "anio").agg(F.sum("choques_ksi").alias("ksi"))
         .toPandas().pivot(index="puma", columns="anio", values="ksi"))
estabilidad = pd.Series({f"{a}-{a + 1}": noche.spearman(anual[a], anual[a + 1]) for a in ANIOS_RECIENTES[:-1]})
tabla(estabilidad.round(4).to_frame("spearman"), "p5_estabilidad")
orden = barrios.sort_values("tasa_ksi_noche", ascending=False)
tabla(orden[["barrio", "borough", "poblacion_2020", "ksi_noche", "tasa_ksi_noche", "pct_ksi_de_noche"]].round(2), "p5_barrios_riesgo")
top10 = orden.head(10).iloc[::-1]
mediana = barrios["tasa_ksi_noche"].median()

celdas = (panel.filter(F.col("anio").isin(ANIOS_RECIENTES)).groupBy("puma", "franja", "dia_noche")
          .agg(F.sum("choques_ksi").alias("ksi"), F.count("*").alias("noches")).toPandas())
celdas["ksi_por_100_noches"] = 100 * celdas["ksi"] / celdas["noches"]
celdas["franja"] = celdas["franja"].map(noche.ETIQUETAS_FRANJA)
celdas["noche"] = celdas["dia_noche"].map(noche.NOCHES)
celdas["barrio"] = celdas["puma"].map(barrios["barrio"])
tabla(celdas.nlargest(20, "ksi_por_100_noches").set_index("puma").round(2), "p5_barrio_noche_franja")

fig, axes = plt.subplots(1, 2, figsize=(14, 5.6), gridspec_kw={"width_ratios": [1.2, 1]})
ax = axes[0]
ax.barh(top10["barrio"].str.slice(0, 40), top10["tasa_ksi_noche"], color=ACENTO)
ax.axvline(mediana, color=TINTA_SECUNDARIA, linestyle="--", linewidth=1)
ax.set_xlabel("choques KSI nocturnos por 100.000 hab. al año (línea: mediana de los 55)")
ax.grid(axis="y", visible=False)
geo.dibujar_pumas(axes[1], pumas, colores_por_valor(barrios["tasa_ksi_noche"]))
axes[1].set_title("Más oscuro = más riesgo", fontsize=10)
fig.suptitle(f"{orden['barrio'].iloc[0]} tiene {dec(orden['tasa_ksi_noche'].iloc[0] / mediana)} veces la tasa mediana; el orden entre "
             f"años consecutivos tiene Spearman de {dec(estabilidad.min(), 2)} a {dec(estabilidad.max(), 2)}",
             x=0.01, ha="left", fontsize=11, fontweight="bold")
fig.tight_layout()
guardar(fig, "10_p5_barrios_riesgo")
plt.show()

# %% [markdown]
# ## P6. ¿Están los arrestos por embriaguez donde está el riesgo?
#
# La brecha de control es el percentil de riesgo nocturno menos el percentil de arrestos por embriaguez. Va de −1 a 1 y un valor alto indica mucho riesgo con poco control. Como los arrestos no traen hora, el control se mide por barrio.

# %%
r_dwi = noche.spearman(barrios["tasa_dwi"], barrios["tasa_ksi_noche"])
r_tra = noche.spearman(barrios["tasa_transito"], barrios["tasa_ksi_noche"])
tabla(pd.DataFrame({"control": ["Arrestos por embriaguez (VTL 1192)", "Arrestos de tránsito (VTL)"],
                    "spearman_con_ksi_nocturnos": [r_dwi, r_tra]}).set_index("control").round(2), "p6_correlaciones")
tabla(barrios.sort_values("brecha", ascending=False)[["barrio", "borough", "poblacion_2020", "tasa_ksi_noche", "tasa_dwi",
      "percentil_riesgo", "percentil_control", "brecha", "brecha_residuo"]].round(3), "p6_brecha")

fig, axes = plt.subplots(1, 2, figsize=(13, 6))
ax = axes[0]
alta = barrios["brecha"] >= 0.5
ax.scatter(barrios["percentil_control"], barrios["percentil_riesgo"], c=np.where(alta, ACENTO, GRIS), s=40)
ax.plot([0, 1], [0, 1], color=TINTA_SECUNDARIA, linestyle=":", linewidth=1)
for _, f in barrios[alta].iterrows():
    ax.annotate(f["barrio"][:26], (f["percentil_control"], f["percentil_riesgo"]), fontsize=7, xytext=(3, 3), textcoords="offset points")
ax.set_xlabel("percentil de control (arrestos por embriaguez por habitante)")
ax.set_ylabel("percentil de riesgo (choques KSI nocturnos por habitante)")
ax = axes[1]
norma = TwoSlopeNorm(vmin=-1, vcenter=0, vmax=1)
cmap = LinearSegmentedColormap.from_list("brecha", [GRIS, "#f2f1ee", ACENTO])
geo.dibujar_pumas(ax, pumas, {p: cmap(norma(v)) for p, v in barrios["brecha"].items()})
ax.set_title("Brecha por barrio (azul = más riesgo que control)", fontsize=10)
fig.suptitle(f"La relación entre control y riesgo es {relacion(r_dwi)} (Spearman {dec(r_dwi, 2)}); barrios con brecha de "
             f"50 puntos o más: {int(alta.sum())}", x=0.01, ha="left", fontsize=11, fontweight="bold")
fig.tight_layout()
guardar(fig, "10_p6_brecha_control")
plt.show()

# %% [markdown]
# ## P7. ¿Cómo cambia el riesgo con el clima y el calendario?
#
# Medimos la probabilidad de que una celda (barrio, noche y franja) tenga un choque KSI entre 2021 y 2025. Son comparaciones simples, y hay que recordar que la temperatura y la lluvia cambian con la época del año.

# %%
pn = panel.filter(F.col("anio").isin(ANIOS_RECIENTES))
condiciones = {
    "Lluvia": F.col("llueve") == 1, "Sin lluvia": F.col("llueve") == 0,
    "Nieve": F.col("nieva") == 1,
    "Festivo": F.col("es_festivo") == 1, "Noche de evento": F.col("es_evento") == 1,
    "Vie→Sáb y Sáb→Dom": F.col("fin_de_semana") == 1, "Otras noches": F.col("fin_de_semana") == 0,
    "< 0 °C": F.col("temperatura") < 0, "≥ 20 °C": F.col("temperatura") >= 20,
}
filas = []
for nombre, cond in condiciones.items():
    r = pn.filter(cond).agg(F.count("*").alias("celdas"), F.sum("obj_ksi").alias("con_ksi"),
                            F.sum("obj_lesion").alias("con_lesion")).first()
    if r["celdas"]:
        filas.append({"condicion": nombre, "celdas": r["celdas"], "con_ksi": r["con_ksi"], "con_lesion": r["con_lesion"]})
p7 = pd.DataFrame(filas)
p7["pct_ksi"] = 100 * p7["con_ksi"] / p7["celdas"]
p7["ic_bajo"], p7["ic_alto"] = [100 * v for v in noche.wilson(p7["con_ksi"], p7["celdas"])]
p7["pct_lesion"] = 100 * p7["con_lesion"] / p7["celdas"]
base_p7 = 100 * pn.agg(F.avg("obj_ksi")).first()[0]
p7["veces_el_promedio"] = p7["pct_ksi"] / base_p7
tabla(p7.set_index("condicion").round(3), "p7_condiciones")

fig, ax = plt.subplots(figsize=(9, 4.2))
o = p7.sort_values("pct_ksi")
ax.barh(o["condicion"], o["pct_ksi"], xerr=[o["pct_ksi"] - o["ic_bajo"], o["ic_alto"] - o["pct_ksi"]],
        color=np.where(o["ic_bajo"] > base_p7, ACENTO, GRIS), error_kw={"elinewidth": 0.8, "ecolor": TINTA_SECUNDARIA})
ax.axvline(base_p7, color=TINTA_SECUNDARIA, linestyle="--", linewidth=1)
ax.set_xlabel("% de celdas con al menos un choque KSI (azul: por encima del promedio con 95 % de confianza)")
claras = o[o["ic_bajo"] > base_p7]
titulo = (f"«{claras.iloc[-1]['condicion']}» es la condición de mayor riesgo: {dec(claras.iloc[-1]['veces_el_promedio'], 2)} veces el promedio"
          if len(claras) else "Ninguna condición supera el promedio con 95 % de confianza")
ax.set_title(titulo, pad=12, fontsize=11)
ax.grid(axis="y", visible=False)
guardar(fig, "10_p7_condiciones")
plt.show()

# %% [markdown]
# ## P8. ¿Qué barrios priorizar y con qué intervención?
#
# Son prioritarios los barrios que están en el cuarto superior de los dos indicadores entre 2021 y 2025. La intervención depende de la brecha de P6. Con una brecha de 0,25 o más proponemos controles preventivos de alcohol y velocidad. En los demás el control ya está más cerca del riesgo, por eso proponemos rediseño vial y alternativas al arresto por delitos menores.

# %%
arr_puma = (arrestos.filter(F.col("puma").isNotNull() & F.col("anio").isin(ANIOS_RECIENTES))
            .groupBy("puma").count().toPandas().set_index("puma")["count"])
p8 = barrios[["barrio", "borough", "poblacion_2020", "ksi_todas_horas", "brecha"]].copy()
p8["arrestos_por_100k"] = arr_puma / N_RECIENTES / p8["poblacion_2020"] * 1e5
p8["ksi_por_100k"] = p8["ksi_todas_horas"] / N_RECIENTES / p8["poblacion_2020"] * 1e5
u_arr, u_ksi = p8["arrestos_por_100k"].quantile(0.75), p8["ksi_por_100k"].quantile(0.75)
p8["prioritario"] = (p8["arrestos_por_100k"] > u_arr) & (p8["ksi_por_100k"] > u_ksi)
p8["intervencion"] = np.where(~p8["prioritario"], "", np.where(p8["brecha"] >= 0.25,
                              "Controles preventivos de alcohol y velocidad", "Rediseño vial y alternativas al arresto"))
tabla(p8.sort_values(["prioritario", "ksi_por_100k"], ascending=False).round(2), "p8_prioridad")
r_p8 = noche.spearman(p8["arrestos_por_100k"], p8["ksi_por_100k"])

fig, ax = plt.subplots(figsize=(10, 5.6))
colores = np.select([p8["intervencion"].str.startswith("Controles"), p8["intervencion"].str.startswith("Rediseño")],
                    [ACENTO, CATEGORICA[1]], GRIS)
ax.scatter(p8["arrestos_por_100k"], p8["ksi_por_100k"], c=colores, s=40)
ax.axvline(u_arr, color=TINTA_SECUNDARIA, linestyle="--", linewidth=0.8)
ax.axhline(u_ksi, color=TINTA_SECUNDARIA, linestyle="--", linewidth=0.8)
for _, f in p8[p8["prioritario"]].iterrows():
    ax.annotate(f["barrio"][:28], (f["arrestos_por_100k"], f["ksi_por_100k"]), fontsize=7, xytext=(3, 3), textcoords="offset points")
ax.scatter([], [], color=ACENTO, label="Controles preventivos de alcohol y velocidad")
ax.scatter([], [], color=CATEGORICA[1], label="Rediseño vial y alternativas al arresto")
ax.legend(fontsize=8, loc="lower right")
ax.set_xscale("log")
ax.xaxis.set_major_formatter(FuncFormatter(miles))
ax.set_xlabel("arrestos por 100.000 hab. al año (escala log)")
ax.set_ylabel("choques KSI por 100.000 hab. al año")
ax.set_title(f"Barrios en el cuarto superior de ambos: {int(p8['prioritario'].sum())}; la relación entre los dos indicadores "
             f"es {relacion(r_p8)} (Spearman {dec(r_p8, 2)})", pad=12, fontsize=10)
guardar(fig, "10_p8_prioridad")
plt.show()

# %% [markdown]
# ## Conclusiones
#
# Ocho barrios quedan como prioritarios, seis para controles preventivos y dos para rediseño vial con alternativas al arresto. De noche ocurre el 44,9 % de las muertes, y los arrestos por embriaguez casi no se relacionan con el riesgo nocturno de cada barrio (Spearman 0,06). Las tablas de cada pregunta están en `figuras/` como `tabla_10_p*.csv`.
