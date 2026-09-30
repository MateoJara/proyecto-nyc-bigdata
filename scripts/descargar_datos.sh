#!/usr/bin/env bash
# descargar_datos.sh: baja las fuentes de NYC Open Data y las sube a HDFS (capa raw).
#
# Se corre en VM1 con el usuario del cluster, cuando HDFS ya está funcionando:
#     sudo -iu bigdata bash /opt/proyecto-nyc-bigdata/scripts/descargar_datos.sh            # todas
#     sudo -iu bigdata bash /opt/proyecto-nyc-bigdata/scripts/descargar_datos.sh choques    # una sola
#
# Notas:
#   - Se baja cada dataset completo y el filtro de años se hace después en Spark. Así
#     raw es una copia fiel de la fuente, no dependemos de los nombres de columnas
#     para filtrar y el filtro queda documentado en el cluster.
#   - Cada archivo se descarga primero como .part y solo se renombra si terminó bien.
#     Si algo falla, se vuelve a correr y se salta lo que ya está descargado.
#   - Se guarda un manifiesto con fecha, tamaño, líneas y SHA-256 de cada archivo,
#     que sirve como evidencia del origen de los datos en el numeral 3.
set -euo pipefail

RAIZ="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=../infra/cluster.env
source "${RAIZ}/infra/cluster.env"
[ -f "${RAIZ}/.env" ] && set -a && source "${RAIZ}/.env" && set +a   # SOCRATA_APP_TOKEN opcional

LOCAL="${DATA_DIR}/local/raw"
HDFS_RAW="/nyc/raw"
MANIFIESTO="${LOCAL}/manifiesto.tsv"
HDFS="${INSTALL_DIR}/hadoop/bin/hdfs"
BASE="${NYC_BASE_URL:-https://data.cityofnewyork.us}"   # sobrescribible solo para pruebas

# nombre | id de NYC Open Data | formato | descripción
FUENTES=(
  "arrestos_historico|8h9b-rp9u|csv|NYPD Arrests Data (Historic)"
  "arrestos_2026|uip8-fykc|csv|NYPD Arrest Data (Year to Date)"
  "choques|h9gi-nx95|csv|Motor Vehicle Collisions - Crashes"
  "vehiculos|bm4k-52h4|csv|Motor Vehicle Collisions - Vehicles"
  "pobreza|cts7-vksw|csv|NYCgov Poverty Measure Data (2018)"
  "sat_2012|f9bf-2cp4|csv|2012 SAT Results"
  "puma_2010|k2r4-n9ax|geojson|2010 Public Use Microdata Areas (PUMAs)"
)

info() { printf '\033[1;34m[INFO]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[ OK ]\033[0m %s\n' "$*"; }
falla(){ printf '\033[1;31m[FALLA]\033[0m %s\n' "$*" >&2; }

url_de() {  # url_de <id> <formato>
  case "$2" in
    csv)     echo "${BASE}/api/views/$1/rows.csv?accessType=DOWNLOAD" ;;
    geojson) echo "${BASE}/api/geospatial/$1?method=export&format=GeoJSON" ;;
  esac
}

descargar() {  # descargar <nombre> <id> <formato> <descripción>
  local nombre=$1 id=$2 fmt=$3 desc=$4
  local archivo="${LOCAL}/${nombre}.${fmt}" url
  url=$(url_de "$id" "$fmt")

  if [ -f "$archivo" ]; then
    info "${nombre}: ya descargado ($(du -h "$archivo" | cut -f1)), se omite."
  else
    info "${nombre}: descargando ${desc} (${id}) ..."
    rm -f "${archivo}.part"
    local cabecera=()
    [ -n "${SOCRATA_APP_TOKEN:-}" ] && cabecera=(-H "X-App-Token: ${SOCRATA_APP_TOKEN}")
    if ! curl -fL --retry 5 --retry-delay 15 --retry-all-errors --connect-timeout 30 \
         "${cabecera[@]}" -o "${archivo}.part" "$url"; then
      # Algunos conjuntos geoespaciales solo exponen el endpoint /resource
      if [ "$fmt" = "geojson" ] && curl -fL --retry 3 -o "${archivo}.part" "${BASE}/resource/${id}.geojson"; then
        :
      else
        falla "${nombre}: no se pudo descargar ${url}"; rm -f "${archivo}.part"; return 1
      fi
    fi
    mv "${archivo}.part" "$archivo"
  fi

  local bytes lineas sha destino
  bytes=$(stat -c %s "$archivo")
  if ! awk -F'\t' -v n="$nombre" '$1 == n {e=1} END {exit !e}' "$MANIFIESTO"; then
    lineas=$(wc -l < "$archivo")
    sha=$(sha256sum "$archivo" | cut -d' ' -f1)
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$nombre" "$id" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      "$bytes" "$lineas" "$sha" "$url" >> "$MANIFIESTO"
  fi

  # Subir a HDFS solo si no está ya con el mismo tamaño (reejecutar no resube GB).
  destino="${HDFS_RAW}/${nombre}/$(basename "$archivo")"
  if [ "$("$HDFS" dfs -stat %b "$destino" 2>/dev/null || echo -1)" != "$bytes" ]; then
    "$HDFS" dfs -mkdir -p "${HDFS_RAW}/${nombre}"
    "$HDFS" dfs -put -f "$archivo" "$destino"
  fi
  ok "${nombre}: $(numfmt --to=iec "$bytes") -> hdfs://${destino}"
}

mkdir -p "$LOCAL"
[ -f "$MANIFIESTO" ] || printf 'fuente\tid\tdescargado_utc\tbytes\tlineas\tsha256\turl\n' > "$MANIFIESTO"
"$HDFS" dfs -mkdir -p "$HDFS_RAW"

PEDIDAS=("$@")
ERRORES=0
for f in "${FUENTES[@]}"; do
  IFS='|' read -r nombre id fmt desc <<<"$f"
  if [ ${#PEDIDAS[@]} -eq 0 ] || printf '%s\n' "${PEDIDAS[@]}" | grep -qx "$nombre"; then
    descargar "$nombre" "$id" "$fmt" "$desc" || ERRORES=$((ERRORES + 1))
  fi
done

"$HDFS" dfs -put -f "$MANIFIESTO" "${HDFS_RAW}/manifiesto.tsv"
cp "$MANIFIESTO" "${RAIZ}/fuentes/manifiesto_descarga.tsv"
echo
info "Manifiesto copiado a fuentes/manifiesto_descarga.tsv (se puede subir a git como evidencia)."
"$HDFS" dfs -du -h "$HDFS_RAW"
[ "$ERRORES" -eq 0 ] || { falla "${ERRORES} fuente(s) fallaron; volver a ejecutar para reintentar."; exit 1; }
ok "Descarga completa."
