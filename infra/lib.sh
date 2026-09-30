#!/usr/bin/env bash
# lib.sh: funciones que usan todos los scripts de infra/. Se carga con source.

set -euo pipefail

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=cluster.env
source "${INFRA_DIR}/cluster.env"

info()  { printf '\033[1;34m[INFO]\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m[ OK ]\033[0m %s\n' "$*"; }
aviso() { printf '\033[1;33m[AVISO]\033[0m %s\n' "$*"; }
error() { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

# Pregunta antes de hacer cualquier cambio en el sistema.
# Con ASUME_SI=1 no pregunta (sirve para volver a correr un paso que ya revisamos).
confirmar() {
  [ "${ASUME_SI:-0}" = "1" ] && return 0
  local r
  read -r -p "$1 [s/N] " r
  [[ "$r" =~ ^[sS]$ ]] || error "Cancelado por el usuario."
}

# Variables derivadas de las listas de nodos (la primera VM es el master).
N_NODOS=${#NODOS_IP[@]}
export VM1_IP="${NODOS_IP[0]}"   # usada por los demás scripts

# Verifica que cluster.env no tenga valores sin llenar y que las listas cuadren.
validar_config() {
  local faltan
  faltan=$(grep -vE '^[[:space:]]*#' "${INFRA_DIR}/cluster.env" | grep -E '"COMPLETAR"' | cut -d= -f1 | tr -d ' ' | paste -sd' ' || true)
  [ -z "$faltan" ] || error "Faltan valores en infra/cluster.env: ${faltan}"
  [ "${#NODOS_ALIAS[@]}" -eq "$N_NODOS" ] && [ "${#WORKER_CORES[@]}" -eq "$N_NODOS" ] && [ "${#WORKER_MEMORY[@]}" -eq "$N_NODOS" ] \
    || error "NODOS_IP, NODOS_ALIAS, WORKER_CORES y WORKER_MEMORY deben tener ${N_NODOS} elementos."
}

# Determina qué VM es esta máquina comparando sus IPs con NODOS_IP.
# Deja: IDX (0, 1, 2...), ROL (vm1, vm2...), MI_IP, OTRAS_IPS (las demás VMs).
detectar_rol() {
  local ips i
  ips=$(ip -4 -o addr show | awk '{print $4}' | cut -d/ -f1)
  IDX=""
  for i in "${!NODOS_IP[@]}"; do
    if grep -qx "${NODOS_IP[$i]}" <<<"$ips"; then IDX=$i; fi
  done
  [ -n "$IDX" ] || error "Ninguna IP local coincide con NODOS_IP (${NODOS_IP[*]})."
  ROL="vm$((IDX + 1))"
  MI_IP="${NODOS_IP[$IDX]}"
  OTRAS_IPS=()
  for i in "${!NODOS_IP[@]}"; do [ "$i" = "$IDX" ] || OTRAS_IPS+=("${NODOS_IP[$i]}"); done
  export IDX ROL MI_IP
}

# JAVA_HOME del OpenJDK de la versión pedida, instalado con dnf.
java_home() {
  local d
  for d in /usr/lib/jvm/java-${JAVA_MAJOR}-openjdk /usr/lib/jvm/jre-${JAVA_MAJOR}-openjdk /usr/lib/jvm/java-${JAVA_MAJOR}-openjdk-*; do
    [ -x "$d/bin/java" ] && { readlink -f "$d"; return 0; }
  done
  error "No se encontró OpenJDK ${JAVA_MAJOR} en /usr/lib/jvm (¿se ejecutó 02_base.sh?)."
}

# Descarga un tarball de Apache y verifica su SHA-512 antes de usarlo.
descargar_apache() {
  local url=$1 destino=$2
  if [ ! -f "$destino" ]; then
    # Primero el CDN de Apache (rápido, solo versiones vigentes); si no está, el archivo histórico.
    local cdn=${url/archive.apache.org\/dist/dlcdn.apache.org}
    info "Descargando $(basename "$url") ..."
    if ! curl -fL --retry 3 --retry-delay 5 -o "${destino}.part" "$cdn"; then
      aviso "No está en dlcdn.apache.org; se usa archive.apache.org (puede ser lento)."
      curl -fL --retry 4 --retry-delay 5 -o "${destino}.part" "$url"
    fi
    mv "${destino}.part" "$destino"
  fi
  local esperado real
  esperado=$(curl -fsSL "${url}.sha512" | grep -oiE '[0-9a-f]{128}' | head -1 | tr 'A-F' 'a-f')
  real=$(sha512sum "$destino" | awk '{print $1}')
  [ "$esperado" = "$real" ] || { rm -f "$destino"; error "SHA-512 no coincide para $(basename "$url"). Archivo borrado; reintentar."; }
  ok "Checksum SHA-512 verificado: $(basename "$url")"
}

como_bigdata() { sudo -u "$CLUSTER_USER" -H bash -lc "$*"; }
