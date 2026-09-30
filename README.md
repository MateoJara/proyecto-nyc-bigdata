# Arrestos y accidentes viales en Nueva York con Apache Spark

Proyecto del curso Procesamiento de Alto Volumen de Datos de la Pontificia Universidad Javeriana,
dictado por el profesor Felipe Cuervo Perez. Lo hicimos Mateo Jaramillo Londoño, Martín Sarmiento Galeano y Wilson Muñoz Torres.

El caso: somos un equipo consultor contratado por el estado de Nueva York para armar un plan de
acción basado en datos que ayude a bajar dos indicadores, la cantidad de arrestos y la de
accidentes viales. Seguimos la metodología CRISP-DM y procesamos todo en un cluster de Spark que
montamos en tres máquinas virtuales de la universidad.

## Objetivo

Identificar en qué zonas de la ciudad se concentran los arrestos y los accidentes viales graves
en relación con su población, cómo esa concentración se asocia con la pobreza y el nivel
educativo de cada distrito, y qué circunstancias de cada choque (causas, vehículos involucrados y
franja horaria) elevan el riesgo de que existan heridos, para recomendar al gobierno dónde y
cuándo priorizar sus intervenciones sociales, de seguridad vial y de control.

Un matiz importante: los arrestos miden la actividad de la policía y no la criminalidad real. Más
arrestos en una zona pueden significar más delitos o más presencia policial.

## Entregas

| Entrega | Contenido | Dónde |
|---|---|---|
| Primera (entendimiento del negocio y de los datos) | Documento en PDF y notebooks ejecutados en el cluster | [`entregas/primera_entrega/`](entregas/primera_entrega/) |
| Final (preparación, modelos y resultados) | En desarrollo | |

## Preguntas de negocio

Se responden en la entrega final.

1. ¿Cuál es la tasa de arrestos por cada 100.000 habitantes en cada distrito y en cada barrio, y cómo ha cambiado entre 2019 y 2026?
2. ¿Los distritos con mayor tasa de pobreza presentan también una mayor tasa de arrestos por habitante, y esta relación se mantiene al separar felonías de delitos menores?
3. ¿Qué tipos de delito predominan en los distritos con mayor pobreza frente a los de menor pobreza?
4. ¿Cuáles son los diez barrios con mayor tasa de choques con heridos o muertos?
5. ¿Qué causas del choque y qué tipos de vehículo se asocian con una mayor proporción de choques con heridos o muertos?
6. ¿En qué horas y días se concentran los choques graves, y este patrón es homogéneo entre distritos?
7. ¿Qué barrios combinan una alta tasa de arrestos con una alta tasa de choques graves y deberían priorizarse?
8. ¿Los arrestos por infracciones de tránsito y conducción bajo estado de embriaguez se concentran en las zonas con mayor accidentalidad grave?

## Fuentes de datos

| Conjunto | Fuente | Tamaño aproximado |
|---|---|---|
| Arrestos históricos del NYPD | [NYC Open Data `8h9b-rp9u`](https://data.cityofnewyork.us/d/8h9b-rp9u) | 6,26 millones de filas |
| Arrestos del año en curso | [NYC Open Data `uip8-fykc`](https://data.cityofnewyork.us/d/uip8-fykc) | 142 mil filas |
| Choques de tránsito | [NYC Open Data `h9gi-nx95`](https://data.cityofnewyork.us/d/h9gi-nx95) | 2,27 millones de filas |
| Vehículos involucrados en choques | [NYC Open Data `bm4k-52h4`](https://data.cityofnewyork.us/d/bm4k-52h4) | 4,55 millones de filas |
| Pobreza (microdatos ACS 2018, NYC Opportunity) | [NYC Open Data `cts7-vksw`](https://data.cityofnewyork.us/d/cts7-vksw) | 68 mil personas |
| Resultados del SAT 2012 | [NYC Open Data `f9bf-2cp4`](https://data.cityofnewyork.us/d/f9bf-2cp4) | 478 colegios |
| Límites de los 55 barrios (PUMA 2010) | [NYC Open Data `k2r4-n9ax`](https://data.cityofnewyork.us/d/k2r4-n9ax) | 55 polígonos |
| Población, área y densidad por condado 2018 (scraping) | [NYS Department of Health, Vital Statistics 2018, tabla 2](https://www.health.ny.gov/statistics/vital_statistics/2018/table02.htm) | 62 condados |

El periodo de análisis va de 2019 a 2025 más el primer semestre de 2026, y se trabaja en dos
niveles: los 5 boroughs y los 55 barrios (PUMA).

## Cluster

Spark 4.1.3 en modo standalone sobre tres VMs con Rocky Linux 9 (4 cores y 15,7 GB cada una),
con HDFS 3.5.0 para que los tres workers lean y escriban los mismos datos. En total son 8
executors, 8 cores y 20 GB de memoria para ejecutar. Los datos pasan por cuatro capas en HDFS:
cruda (CSV), bronce (Parquet como texto), plata (tipado y limpio) y oro (agregados).

El diseño, los recursos de cada máquina, los puertos y el registro de la instalación están en
[`docs/arquitectura.md`](docs/arquitectura.md).

## Estructura del repositorio

```
proyecto-nyc-bigdata/
├── docs/          arquitectura del cluster
├── entregas/      documentos entregados y notebooks ejecutados en el cluster
├── figuras/       gráficas y tablas que generan los notebooks
├── fuentes/       página guardada para el scraping (y el manifiesto que deja descargar_datos.sh)
├── infra/         scripts para montar el cluster (cluster.env tiene toda la configuración)
├── notebooks/     notebooks del análisis y módulos comunes (config, codigos, estilo, geo)
└── scripts/       descarga de los datos a HDFS y ejecución de los notebooks
```

Los notebooks se corren en este orden: `00_cluster`, `01_ingesta`, `04_calidad`,
`05_limpieza_inicial`, `02_descripcion`, `03_exploracion` y `06_bono_scraping_poblacion`. La
limpieza (05) va antes de la descripción y la exploración porque esas dos trabajan sobre la capa
plata.

## Cómo reproducirlo

1. Clonar el repositorio en todas las VMs.
2. Correr `infra/00_diagnostico.sh` en cada VM y completar `infra/cluster.env`.
3. Montar el cluster siguiendo la sección 6 de [`docs/arquitectura.md`](docs/arquitectura.md).
4. En VM1, bajar los datos a HDFS:
   `sudo -iu bigdata bash /opt/proyecto-nyc-bigdata/scripts/descargar_datos.sh`
5. Ejecutar los notebooks, desde JupyterLab (túnel SSH a VM1, puerto 8888) o todos seguidos con
   `scripts/ejecutar_notebooks.sh`.
