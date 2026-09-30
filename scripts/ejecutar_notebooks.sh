#!/usr/bin/env bash
# ejecutar_notebooks.sh: corre los notebooks en el cluster, en orden, sin abrir el
# navegador. Los notebooks ya ejecutados (con sus tablas y gráficas) quedan en
# ejecutados/ y las figuras en figuras/. ejecutados/ no se sube a git: los que van
# en cada entrega se copian a entregas/<entrega>/notebooks_ejecutados/.
#
# Uso (en VM1 y en segundo plano, para que no se corte si se cierra la terminal):
#   sudo -iu bigdata bash -c 'nohup bash /opt/proyecto-nyc-bigdata/scripts/ejecutar_notebooks.sh > ~/notebooks.log 2>&1 &'
#   Para correr solo algunos:  ... ejecutar_notebooks.sh 04_calidad 05_limpieza_inicial
#
# El orden no es 01, 02, 03... porque unos dependen de otros:
#   00_cluster          verifica el cluster (evidencia para el documento)
#   01_ingesta          CSV -> Parquet (bronce)
#   04_calidad          mide faltantes sobre bronce
#   05_limpieza_inicial bronce -> plata (usa lo que salió en calidad)
#   02_descripcion      describe bronce y los tipos de plata
#   03_exploracion      gráficas sobre plata
#   06_bono_scraping_poblacion  población oficial por borough (usa plata)
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=../infra/cluster.env
source "${REPO}/infra/cluster.env"
JUPYTER="${JUPYTER:-${VENV_DIR}/bin/jupyter}"
ORDEN=("$@")
[ ${#ORDEN[@]} -gt 0 ] || ORDEN=(00_cluster 01_ingesta 04_calidad 05_limpieza_inicial 02_descripcion 03_exploracion 06_bono_scraping_poblacion)

cd "$REPO"
mkdir -p ejecutados figuras

# Si alguien ejecutó un notebook directamente en notebooks/, esa versión se copia a
# ejecutados/ y se deja el original, para que git pull no choque con cambios locales.
if git rev-parse --git-dir >/dev/null 2>&1; then
  for f in $(git diff --name-only -- 'notebooks/*.ipynb'); do
    cp "$f" "ejecutados/$(basename "$f")"
    git checkout -- "$f"
  done
  if ! git pull -q; then
    echo "[ERROR] No se pudo actualizar el repositorio (git pull). No se ejecuta nada con código viejo."
    echo "        Revisen el mensaje de git de arriba; suele ser un archivo local que choca con uno del repo."
    exit 1
  fi
fi

for nb in "${ORDEN[@]}"; do
  echo "[$(date '+%H:%M:%S')] >>> ${nb}"
  inicio=$(date +%s)
  "$JUPYTER" nbconvert --to notebook --execute \
    --ExecutePreprocessor.timeout=-1 \
    --output-dir "${REPO}/ejecutados" "${REPO}/notebooks/${nb}.ipynb"
  echo "[$(date '+%H:%M:%S')] OK  ${nb} ($(( $(date +%s) - inicio )) s)"
done
echo "Terminaron todos los notebooks."
