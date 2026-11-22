#!/usr/bin/env bash
# ejecutar_entrega_final.sh: corre la entrega final completa en VM1, por pasos.
#
#   bash scripts/ejecutar_entrega_final.sh verificar      # revisa que todo esté listo (no cambia nada)
#   bash scripts/ejecutar_entrega_final.sh pruebas        # pruebas de Python puro (tests/)
#   bash scripts/ejecutar_entrega_final.sh descargar      # choques, vehículos, personas, censo 2020 y clima -> HDFS /nyc/raw/entrega_final
#   bash scripts/ejecutar_entrega_final.sh notebooks 07_fuentes_nuevas 08_entendimiento_noche
#   bash scripts/ejecutar_entrega_final.sh notebooks      # sin nombres: 07 a 11 en orden
#   bash scripts/ejecutar_entrega_final.sh todo           # pruebas, descarga, 07-11 y empaquetar; para si G1 no pasa
#   bash scripts/ejecutar_entrega_final.sh repetir        # como todo, con los datos ya descargados
#   bash scripts/ejecutar_entrega_final.sh empaquetar     # copia resultados a entregas/entrega_final y arma un .tar.gz
#
# Se corre como el usuario del cluster (bigdata) desde la carpeta del proyecto:
#   sudo -iu bigdata bash -c 'cd /opt/nyc-final && bash scripts/ejecutar_entrega_final.sh verificar'
#
# A diferencia de scripts/ejecutar_notebooks.sh, este no hace git pull ni revierte notebooks,
# así que también sirve en una carpeta que no sea un clon de git.
set -euo pipefail

RAIZ="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=../infra/cluster.env
source "${RAIZ}/infra/cluster.env"
HDFS="${INSTALL_DIR}/hadoop/bin/hdfs"
PY="${VENV_DIR}/bin/python"
JUPYTER="${VENV_DIR}/bin/jupyter"
LOGS="${RAIZ}/logs"
mkdir -p "$LOGS" "${RAIZ}/ejecutados" "${RAIZ}/figuras"
LOCAL_FINAL="${DATA_DIR}/local/raw/entrega_final"
HDFS_FINAL="/nyc/raw/entrega_final"       # carpeta propia: no toca los archivos de la primera entrega

info() { printf '\033[1;34m[INFO]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[ OK ]\033[0m %s\n' "$*"; }
mal()  { printf '\033[1;31m[FALLA]\033[0m %s\n' "$*" >&2; FALLAS=$((FALLAS + 1)); }
FALLAS=0

existe_hdfs() { "$HDFS" dfs -test -e "$1" 2>/dev/null; }

verificar() {
  info "Carpeta del proyecto: ${RAIZ}"
  [ "$(id -un)" = "$CLUSTER_USER" ] && ok "usuario ${CLUSTER_USER}" || mal "correr como ${CLUSTER_USER} (sudo -iu ${CLUSTER_USER})"

  # 1. Clave de OpenWeather
  if [ -f "${RAIZ}/.env" ] && grep -q '^OPENWEATHER_API_KEY=[0-9a-f]\{32\}' "${RAIZ}/.env"; then
    ok ".env con OPENWEATHER_API_KEY"
  else
    info "sin OPENWEATHER_API_KEY en ${RAIZ}/.env: se omite el pronóstico (el histórico no la necesita)"
  fi

  # 2. Python y librerías
  [ -x "$PY" ] && ok "Python del entorno: $("$PY" --version 2>&1)" || mal "no existe ${PY}"
  "$PY" - <<'EOF' && ok "librerías de Python (pyspark, pandas, numpy, matplotlib, shapely, requests, nbconvert)" || mal "faltan librerías en el entorno (ver arriba)"
import importlib.util, sys
faltan = [m for m in ["pyspark", "pandas", "numpy", "matplotlib", "shapely", "requests", "pyarrow", "nbconvert", "nbformat"]
          if importlib.util.find_spec(m) is None]
if faltan:
    print("Faltan:", faltan); sys.exit(1)
import pyspark
print("PySpark", pyspark.__version__)
EOF

  # 3. Servicios: HDFS y Spark master
  if "$HDFS" dfs -ls / >/dev/null 2>&1; then ok "HDFS responde"; else mal "HDFS no responde (revisar servicios con infra/07_verificar.sh)"; fi
  if curl -fsS "http://${NODOS_IP[0]}:${SPARK_MASTER_WEBUI_PORT}/json/" >/tmp/master.json 2>/dev/null; then
    vivos=$("$PY" -c "import json;print(sum(w['state']=='ALIVE' for w in json.load(open('/tmp/master.json'))['workers']))")
    [ "$vivos" -ge 3 ] && ok "Spark master con ${vivos} workers vivos" || mal "Spark master con solo ${vivos} workers vivos (deberían ser 3)"
  else
    mal "no responde la UI del Spark master en el puerto ${SPARK_MASTER_WEBUI_PORT}"
  fi

  # 4. Lo que se reutiliza de la primera entrega (arrestos, pobreza y fronteras no cambiaron)
  for t in /nyc/silver/arrestos /nyc/silver/pobreza /nyc/raw/puma_2010; do
    existe_hdfs "$t" && ok "existe ${t}" || mal "no existe ${t} (no correr 01 ni 05 para crearla: avisar al grupo)"
  done

  # 5. Salida a internet desde VM1
  for u in https://data.cityofnewyork.us https://www2.census.gov \
           https://archive-api.open-meteo.com https://api.openweathermap.org; do
    if curl -sS -o /dev/null --max-time 15 "$u"; then ok "internet: ${u}"; else mal "sin salida a ${u}"; fi
  done

  # 6. Espacio en disco local (personas ocupa ~1,5 GB en CSV)
  libre=$(df -BG --output=avail "${DATA_DIR}" | tail -1 | tr -dc '0-9')
  [ "${libre:-0}" -ge 5 ] && ok "espacio libre en ${DATA_DIR}: ${libre} GB" || mal "poco espacio en ${DATA_DIR}: ${libre} GB (se necesitan ~5 GB)"

  echo
  if [ "$FALLAS" -eq 0 ]; then ok "Todo listo para ejecutar."; else mal "Hay ${FALLAS} problema(s); resolverlos antes de seguir."; exit 1; fi
}

pruebas() {
  "$PY" "${RAIZ}/tests/probar_noche.py"
  "$PY" "${RAIZ}/tests/probar_extraer_clima.py"
}

descargar() {
  info "Descargando choques, vehículos, personas, censo 2020 y clima ..."
  # El clima va en paralelo con las tablas. La clave la lee extraer_clima.py del .env.
  "$PY" -u "${RAIZ}/scripts/descargar_entrega_final.py" --salida "$LOCAL_FINAL" > "${LOGS}/descarga.log" 2>&1 &
  local tablas=$!
  "$PY" -u "${RAIZ}/scripts/extraer_clima.py" --salida "${LOCAL_FINAL}/clima" > "${LOGS}/descarga_clima.log" 2>&1 &
  local clima=$!
  local error=0
  wait "$clima" || error=1
  cat "${LOGS}/descarga_clima.log"
  wait "$tablas" || error=1
  cat "${LOGS}/descarga.log"
  if [ "$error" -ne 0 ]; then mal "descarga incompleta: volver a correr este paso (retoma lo que falta)"; exit 1; fi

  info "Subiendo a ${HDFS_FINAL} ..."
  "$HDFS" dfs -mkdir -p "$HDFS_FINAL"
  for c in choques vehiculos personas censo2020 clima; do
    "$HDFS" dfs -put -f "${LOCAL_FINAL}/${c}" "${HDFS_FINAL}/"
  done
  "$HDFS" dfs -put -f "${LOCAL_FINAL}/manifiesto.tsv" "${HDFS_FINAL}/manifiesto.tsv"
  cp "${LOCAL_FINAL}/manifiesto.tsv" "${RAIZ}/fuentes/manifiesto_entrega_final.tsv"
  "$HDFS" dfs -du -h "$HDFS_FINAL"
  ok "Descarga completa."
}

notebooks() {
  local lista=("$@")
  [ ${#lista[@]} -gt 0 ] || lista=(07_fuentes_nuevas 08_entendimiento_noche 09_capa_oro 10_respuestas 11_modelos)
  for nb in "${lista[@]}"; do
    [ -f "${RAIZ}/notebooks/${nb}.ipynb" ] || { mal "no existe notebooks/${nb}.ipynb"; exit 1; }
    info "$(date '+%H:%M:%S') >>> ${nb}"
    inicio=$(date +%s)
    # Se ejecuta en notebooks/ para que las rutas ../figuras funcionen; la copia ejecutada va a ejecutados/
    if "$PY" "${RAIZ}/herramientas/ejecutar_notebook.py" "${RAIZ}/notebooks/${nb}.ipynb" \
          "${RAIZ}/ejecutados/${nb}.ipynb" > "${LOGS}/${nb}.log" 2>&1; then
      ok "${nb} ($(( $(date +%s) - inicio )) s). Notebook ejecutado en ejecutados/${nb}.ipynb"
    else
      mal "${nb} falló. Ver ${LOGS}/${nb}.log y la celda con error en ejecutados/${nb}.ipynb"
      tail -30 "${LOGS}/${nb}.log" >&2
      exit 1
    fi
  done
}

empaquetar() {
  local destino="${RAIZ}/entregas/entrega_final/notebooks_ejecutados"
  mkdir -p "$destino"
  cp "${RAIZ}"/ejecutados/0[7-9]_*.ipynb "${RAIZ}"/ejecutados/1[01]_*.ipynb "$destino"/ 2>/dev/null || true
  local paquete="${RAIZ}/entrega_final_resultados_$(date +%Y%m%d_%H%M).tar.gz"
  local extras=()
  for f in fuentes/manifiesto_entrega_final.tsv; do
    [ -f "${RAIZ}/${f}" ] && extras+=("$f")
  done
  tar -czf "$paquete" -C "$RAIZ" entregas/entrega_final figuras logs notebooks "${extras[@]}"
  ok "Resultados en ${paquete} ($(du -h "$paquete" | cut -f1)). Bajarlo al portátil con scp."
}

compuerta_g1() {
  # Sale con 0 solo si el intervalo de la razón noche/día queda sobre 1 con las dos variantes de KSI
  "$PY" - "${RAIZ}/figuras/tabla_08_compuerta_g1.csv" <<'EOF'
import csv, sys
ic = {f["variable"]: float(f["ic_bajo"]) for f in csv.DictReader(open(sys.argv[1]))}
sys.exit(0 if ic["ksi"] > 1 and ic["ksi_amplia"] > 1 else 1)
EOF
}

todo() {
  info "Inicio: $(date '+%d-%m %H:%M')"
  pruebas
  descargar
  correr_y_empaquetar
}

repetir() {
  info "Repetición sin descarga: $(date '+%d-%m %H:%M')"
  correr_y_empaquetar
}

correr_y_empaquetar() {
  notebooks 07_fuentes_nuevas 08_entendimiento_noche
  if ! compuerta_g1; then
    mal "G1 NO SUPERADA: se detiene antes del 09. El grupo decide con figuras/tabla_08_compuerta_g1.csv."
    exit 1
  fi
  ok "G1 superada"
  notebooks 09_capa_oro 10_respuestas 11_modelos
  empaquetar
  ok "TODO TERMINADO: $(date '+%d-%m %H:%M')"
}

paso="${1:-}"; shift || true
case "$paso" in
  verificar)  verificar ;;
  pruebas)    pruebas ;;
  descargar)  descargar ;;
  notebooks)  notebooks "$@" ;;
  empaquetar) empaquetar ;;
  todo)       todo ;;
  repetir)    repetir ;;
  *) sed -n '2,14p' "$0"; exit 1 ;;
esac
