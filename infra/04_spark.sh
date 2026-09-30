#!/usr/bin/env bash
# 04_spark.sh: instala Spark y arma su configuración según la VM.
# Se corre en todas las VMs:   sudo bash infra/04_spark.sh
#
#   - Descarga Spark, revisa el SHA-512 y lo deja en /opt/spark, que es un enlace a la
#     carpeta de la versión (así se puede cambiar de versión sin tocar rutas).
#   - conf/spark-env.sh: variables de cada proceso (IP, cores y memoria del worker).
#   - conf/spark-defaults.conf: lo que usan por defecto las aplicaciones (puertos fijos
#     del driver y del block manager, tamaño de los executors, event log).
#
# No hay script 03: era el del firewall y se quitó, porque las VMs de la universidad
# traen firewalld apagado y decidimos dejarlo así.
source "$(dirname "$0")/lib.sh"
validar_config
detectar_rol
[ "$(id -u)" -eq 0 ] || error "Ejecutar con sudo."

JH=$(java_home)
PAQUETE="spark-${SPARK_VERSION}-bin-hadoop3"
URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${PAQUETE}.tgz"
SPARK_HOME="${INSTALL_DIR}/spark"

W_CORES=${WORKER_CORES[$IDX]}
W_MEM=${WORKER_MEMORY[$IDX]}

cat <<EOF
Se instalará Spark ${SPARK_VERSION} en ${INSTALL_DIR}/${PAQUETE} (enlace ${SPARK_HOME}).
Worker de esta VM (${ROL}): ${W_CORES} cores, ${W_MEM} de memoria.
Se creará /etc/profile.d/nyc-bigdata.sh con SPARK_HOME, JAVA_HOME y PATH.
EOF
confirmar "¿Continuar?"

descargar_apache "$URL" "/tmp/${PAQUETE}.tgz"
if [ ! -d "${INSTALL_DIR}/${PAQUETE}" ]; then
  tar -xzf "/tmp/${PAQUETE}.tgz" -C "$INSTALL_DIR"
fi
ln -sfn "${INSTALL_DIR}/${PAQUETE}" "$SPARK_HOME"
install -d -o "$CLUSTER_USER" -g "$CLUSTER_USER" "${SPARK_HOME}/logs"

HADOOP_CONF="${INSTALL_DIR}/hadoop/etc/hadoop"

# ---- spark-env.sh ------------------------------------------------------------
cat > "${SPARK_HOME}/conf/spark-env.sh" <<EOF
# Generado por infra/04_spark.sh para ${ROL}. No editar a mano: cambiar cluster.env y reejecutar.
export JAVA_HOME=${JH}
# IP con la que se anuncia este nodo. Sin esto el hostname puede resolver a
# 127.0.0.1 y las otras VMs terminan intentando conectarse a sí mismas.
export SPARK_LOCAL_IP=${MI_IP}
# Que se anuncie con la IP y no con el hostname: en estas VMs el hostname resuelve
# a una IPv6 de enlace local (fe80::...) que desde las otras máquinas no sirve.
export SPARK_LOCAL_HOSTNAME=${MI_IP}
export SPARK_MASTER_HOST=${VM1_IP}
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT}
export SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT}
export SPARK_WORKER_CORES=${W_CORES}
export SPARK_WORKER_MEMORY=${W_MEM}
export SPARK_WORKER_PORT=${SPARK_WORKER_PORT}
export SPARK_WORKER_WEBUI_PORT=${SPARK_WORKER_WEBUI_PORT}
export SPARK_WORKER_DIR=${DATA_DIR}/spark-work
# Shuffle y spill van a la partición grande, no a /tmp.
export SPARK_LOCAL_DIRS=${DATA_DIR}/spark-local
# Borra las carpetas de aplicaciones que terminaron hace más de un día.
export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.appDataTtl=86400"
# Si su puerto está ocupado, master, worker e History Server fallan en vez de pasarse
# a otro puerto (si el master no queda en el 7077 nadie lo encuentra).
export SPARK_DAEMON_JAVA_OPTS="-Dspark.port.maxRetries=0"
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://${DATA_DIR}/spark-events -Dspark.history.ui.port=18080"
export PYSPARK_PYTHON=${VENV_DIR}/bin/python
export PYSPARK_DRIVER_PYTHON=${VENV_DIR}/bin/python
EOF
if [ "$USE_HDFS" = "true" ]; then
  echo "export HADOOP_CONF_DIR=${HADOOP_CONF}" >> "${SPARK_HOME}/conf/spark-env.sh"
fi

# ---- spark-defaults.conf -----------------------------------------------------
cat > "${SPARK_HOME}/conf/spark-defaults.conf" <<EOF
# Generado por infra/04_spark.sh. Valores por defecto de toda aplicación Spark.
spark.master                      spark://${VM1_IP}:${SPARK_MASTER_PORT}

# El driver corre en VM1 (Jupyter) y los executors de VM2 y VM3 se tienen que poder
# conectar a él, por eso sus puertos van fijos y abiertos en el firewall.
spark.driver.host                 ${VM1_IP}
spark.driver.port                 ${SPARK_DRIVER_PORT}
spark.driver.blockManager.port    ${SPARK_DRIVER_BM_PORT}
spark.blockManager.port           ${SPARK_BM_PORT}
spark.ui.port                     ${SPARK_UI_PORT}
spark.port.maxRetries             10

spark.driver.memory               ${DRIVER_MEMORY}
spark.driver.maxResultSize        1g
spark.executor.cores              ${EXECUTOR_CORES}
spark.executor.memory             ${EXECUTOR_MEMORY}
spark.sql.shuffle.partitions      ${SHUFFLE_PARTITIONS}
spark.sql.adaptive.enabled        true
spark.serializer                  org.apache.spark.serializer.KryoSerializer
spark.sql.session.timeZone        America/New_York

spark.pyspark.python              ${VENV_DIR}/bin/python
spark.pyspark.driver.python       ${VENV_DIR}/bin/python

# Guarda el historial para ver los DAG y los tiempos después de cerrar el notebook
spark.eventLog.enabled            true
spark.eventLog.dir                file://${DATA_DIR}/spark-events
EOF
chown -R "$CLUSTER_USER:$CLUSTER_USER" "${SPARK_HOME}/conf"

# ---- PySpark dentro del venv ---------------------------------------------------
# No se usa "pip install pyspark" porque podría instalar otra versión; se enlaza
# el PySpark que viene con Spark.
SITE=$("${VENV_DIR}/bin/python" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
PY4J=$(ls "${SPARK_HOME}"/python/lib/py4j-*-src.zip | head -1)
printf '%s\n%s\n' "${SPARK_HOME}/python" "$PY4J" > "${SITE}/spark-local.pth"
chown "$CLUSTER_USER:$CLUSTER_USER" "${SITE}/spark-local.pth"

# ---- Variables de entorno para todos los usuarios -----------------------------------
cat > /etc/profile.d/nyc-bigdata.sh <<EOF
export JAVA_HOME=${JH}
export SPARK_HOME=${SPARK_HOME}
export HADOOP_HOME=${INSTALL_DIR}/hadoop
export HADOOP_CONF_DIR=${HADOOP_CONF}
export PYSPARK_PYTHON=${VENV_DIR}/bin/python
export PATH=\$PATH:${SPARK_HOME}/bin:${INSTALL_DIR}/hadoop/bin
EOF

ok "Spark instalado: $("${SPARK_HOME}/bin/spark-submit" --version 2>&1 | grep -m1 -oE 'version [0-9.]+')"
ok "PySpark desde el venv: $(sudo -u "$CLUSTER_USER" "${VENV_DIR}/bin/python" -c 'import pyspark; print(pyspark.__version__)')"
