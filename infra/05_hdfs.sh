#!/usr/bin/env bash
# 05_hdfs.sh: instala Hadoop (solo HDFS) para que todos los workers lean los mismos datos.
# Se corre en todas las VMs, solo si USE_HDFS="true":   sudo bash infra/05_hdfs.sh
#
#   VM1: NameNode (sabe qué bloques forman cada archivo y en qué nodo está cada uno)
#        y DataNode.
#   VM2 y VM3: DataNode.
#   Replicación 2: cada bloque de 128 MB queda en 2 de las 3 VMs. Si se cae una VM no
#   se pierden datos, y se usa 2/3 del disco que se usaría con replicación 3.
source "$(dirname "$0")/lib.sh"
validar_config
detectar_rol
[ "$(id -u)" -eq 0 ] || error "Ejecutar con sudo."
[ "$USE_HDFS" = "true" ] || { aviso "USE_HDFS=false en cluster.env: no se instala HDFS."; exit 0; }

JH=$(java_home)
PAQUETE="hadoop-${HADOOP_VERSION}"
URL="https://archive.apache.org/dist/hadoop/common/${PAQUETE}/${PAQUETE}.tar.gz"
HADOOP_HOME="${INSTALL_DIR}/hadoop"
CONF="${HADOOP_HOME}/etc/hadoop"

cat <<EOF
Se instalará Hadoop ${HADOOP_VERSION} en ${INSTALL_DIR}/${PAQUETE} (enlace ${HADOOP_HOME}).
Rol HDFS de esta VM (${ROL}): $([ "$ROL" = vm1 ] && echo "NameNode + DataNode" || echo "DataNode")
Datos de HDFS en ${DATA_DIR}/hdfs
EOF
confirmar "¿Continuar?"

descargar_apache "$URL" "/tmp/${PAQUETE}.tar.gz"
if [ ! -d "${INSTALL_DIR}/${PAQUETE}" ]; then
  tar -xzf "/tmp/${PAQUETE}.tar.gz" -C "$INSTALL_DIR"
  rm -rf "${INSTALL_DIR}/${PAQUETE}/share/doc"   # ~500 MB de documentación que no se usa
fi
ln -sfn "${INSTALL_DIR}/${PAQUETE}" "$HADOOP_HOME"
chown -R "$CLUSTER_USER:$CLUSTER_USER" "${INSTALL_DIR}/${PAQUETE}"

cat > "${CONF}/core-site.xml" <<EOF
<?xml version="1.0"?>
<!-- Generado por infra/05_hdfs.sh -->
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${VM1_IP}:${HDFS_NN_PORT}</value>
  </property>
</configuration>
EOF

cat > "${CONF}/hdfs-site.xml" <<EOF
<?xml version="1.0"?>
<!-- Generado por infra/05_hdfs.sh -->
<configuration>
  <property><name>dfs.replication</name><value>${HDFS_REPLICATION}</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://${DATA_DIR}/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://${DATA_DIR}/hdfs/datanode</value></property>
  <property><name>dfs.namenode.http-address</name><value>${VM1_IP}:${HDFS_NN_HTTP_PORT}</value></property>
  <!-- Los DataNodes se anuncian por IP: no dependemos del DNS inverso de la universidad -->
  <property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>
  <property><name>dfs.datanode.address</name><value>${MI_IP}:9866</value></property>
  <property><name>dfs.datanode.ipc.address</name><value>${MI_IP}:9867</value></property>
  <property><name>dfs.datanode.http.address</name><value>${MI_IP}:9864</value></property>
</configuration>
EOF

sed -i '/^# --- Agregado por infra\/05_hdfs.sh ---$/,$d' "${CONF}/hadoop-env.sh"   # idempotente
cat >> "${CONF}/hadoop-env.sh" <<EOF
# --- Agregado por infra/05_hdfs.sh ---
export JAVA_HOME=${JH}
export HADOOP_HEAPSIZE_MAX=1g
export HADOOP_LOG_DIR=${HADOOP_HOME}/logs
EOF
install -d -o "$CLUSTER_USER" -g "$CLUSTER_USER" "${HADOOP_HOME}/logs"
chown -R "$CLUSTER_USER:$CLUSTER_USER" "$CONF"

# Formatear el NameNode: solo una vez, solo en VM1.
if [ "$ROL" = "vm1" ]; then
  if [ -f "${DATA_DIR}/hdfs/namenode/current/VERSION" ]; then
    ok "El NameNode ya estaba formateado: no se toca."
  else
    confirmar "Formatear el NameNode en ${DATA_DIR}/hdfs/namenode (HDFS vacío, primera vez)?"
    como_bigdata "${HADOOP_HOME}/bin/hdfs namenode -format -nonInteractive -clusterId nyc-bigdata"
    ok "NameNode formateado."
  fi
fi

ok "Hadoop instalado: $(como_bigdata "${HADOOP_HOME}/bin/hadoop version" | head -1)"
