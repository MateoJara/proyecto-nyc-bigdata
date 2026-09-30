#!/usr/bin/env bash
# 02_base.sh: paquetes base, usuario del cluster, /etc/hosts y entorno de Python.
# Se corre en todas las VMs:   sudo bash infra/02_base.sh
#
#   - Instala OpenJDK 17 (lo piden Spark 4 y Hadoop 3.5) y Python 3.11, porque
#     PySpark 4.1 necesita 3.10 o más y Rocky viene con 3.9.
#   - Crea el usuario "bigdata". Todos los procesos corren con ese usuario en todas
#     las VMs, así no hay problemas de permisos en HDFS ni en los archivos.
#   - Agrega nyc-vm1, nyc-vm2 y nyc-vm3 a /etc/hosts para no depender del DNS de la universidad.
#   - Crea el entorno virtual de Python en la misma ruta en todas las VMs, porque los
#     executors de VM2 y VM3 también corren Python y necesitan las mismas librerías.
source "$(dirname "$0")/lib.sh"
validar_config
detectar_rol
[ "$(id -u)" -eq 0 ] || error "Ejecutar con sudo."

info "Esta VM es ${ROL} (${MI_IP}). Las demás: ${OTRAS_IPS[*]}."
cat <<EOF

Se van a hacer estos cambios en el sistema:
  1. dnf install java-${JAVA_MAJOR}-openjdk-headless ${PYTHON_BIN} ${PYTHON_BIN}-pip tar curl rsync
  2. Crear el usuario '${CLUSTER_USER}' (si no existe)
  3. Agregar a /etc/hosts los ${N_NODOS} nodos (${NODOS_ALIAS[*]}), con copia de respaldo
  4. Crear ${DATA_DIR}/{hdfs,spark-local,spark-work,spark-events,local} y ${VENV_DIR}
  5. Instalar las librerías de requirements.txt en ${VENV_DIR}
$([ "$ROL" = vm1 ] && echo "  6. Clonar el repositorio en ${REPO_DIR} (carpeta de trabajo de Jupyter)")
EOF
confirmar "¿Continuar?"

# 1. Paquetes
dnf install -y "java-${JAVA_MAJOR}-openjdk-headless" "${PYTHON_BIN}" "${PYTHON_BIN}-pip" tar curl rsync procps-ng git
JH=$(java_home)
ok "Java: $("$JH/bin/java" -version 2>&1 | head -1)  (JAVA_HOME=${JH})"

# 2. Usuario de servicio
if ! id "$CLUSTER_USER" >/dev/null 2>&1; then
  useradd -m -s /bin/bash "$CLUSTER_USER"
  ok "Usuario ${CLUSTER_USER} creado."
else
  ok "Usuario ${CLUSTER_USER} ya existía."
fi

# 3. /etc/hosts
# Se borra y se vuelve a escribir solo el bloque marcado, para que correr el script otra vez
# (o agregar una VM) no duplique líneas.
cp -a /etc/hosts "/etc/hosts.bak-nyc-$(date +%Y%m%d%H%M%S)"
sed -i '/^# --- cluster proyecto-nyc-bigdata ---$/,/^# --- fin cluster ---$/d' /etc/hosts
{
  echo "# --- cluster proyecto-nyc-bigdata ---"
  for i in "${!NODOS_IP[@]}"; do echo "${NODOS_IP[$i]} ${NODOS_ALIAS[$i]}"; done
  echo "# --- fin cluster ---"
} >> /etc/hosts
ok "/etc/hosts actualizado."

# 4. Directorios de datos
install -d -o "$CLUSTER_USER" -g "$CLUSTER_USER" \
  "$DATA_DIR" "$DATA_DIR/hdfs" "$DATA_DIR/hdfs/datanode" \
  "$DATA_DIR/spark-local" "$DATA_DIR/spark-work" "$DATA_DIR/spark-events" "$DATA_DIR/local"
[ "$ROL" = "vm1" ] && install -d -o "$CLUSTER_USER" -g "$CLUSTER_USER" "$DATA_DIR/hdfs/namenode"
ok "Directorios en ${DATA_DIR} listos."

# 5. Entorno virtual Python
if [ ! -x "${VENV_DIR}/bin/python" ]; then
  install -d -o "$CLUSTER_USER" -g "$CLUSTER_USER" "$VENV_DIR"
  como_bigdata "${PYTHON_BIN} -m venv ${VENV_DIR}"
fi
install -m 644 "${INFRA_DIR}/requirements.txt" /tmp/nyc-requirements.txt
como_bigdata "${VENV_DIR}/bin/pip install --upgrade pip && ${VENV_DIR}/bin/pip install -r /tmp/nyc-requirements.txt"
como_bigdata "${VENV_DIR}/bin/pip freeze" > "${INFRA_DIR}/pip-freeze-${ROL}.txt"
ok "Entorno Python listo: $("${VENV_DIR}/bin/python" --version). Versiones en infra/pip-freeze-${ROL}.txt"
info "Comparen los pip-freeze-vmN.txt de todas las VMs: deben ser idénticos."

# 6. Copia del repositorio para Jupyter (solo VM1), a nombre del usuario del cluster
if [ "$ROL" = "vm1" ]; then
  if [ -d "${REPO_DIR}/.git" ]; then
    ok "El repositorio ya estaba en ${REPO_DIR}."
  else
    install -d -o "$CLUSTER_USER" -g "$CLUSTER_USER" "$REPO_DIR"
    como_bigdata "git clone https://github.com/MateoJara/proyecto-nyc-bigdata.git ${REPO_DIR}"
    ok "Repositorio clonado en ${REPO_DIR}."
  fi
fi
