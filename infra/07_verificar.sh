#!/usr/bin/env bash
# 07_verificar.sh: prueba completa del cluster. Se corre en VM1:
#     sudo bash infra/07_verificar.sh
#
#   1. Que el master vea todos los workers con los cores y la memoria que deben tener.
#   2. Que HDFS tenga todos los DataNodes vivos y que un archivo de prueba quede replicado.
#   3. Que un job de PySpark reparta tareas en todas las VMs y lea desde HDFS.
# Lo único que escribe es un archivo de prueba en HDFS (/tmp/prueba_cluster), que se borra al final.
source "$(dirname "$0")/lib.sh"
validar_config
detectar_rol
[ "$ROL" = "vm1" ] || error "Ejecutar en VM1 (donde está el master)."

SPARK_HOME="${INSTALL_DIR}/spark"
HADOOP_HOME="${INSTALL_DIR}/hadoop"
FALLAS=0

info "1. Workers registrados en el master"
"${VENV_DIR}/bin/python" - "http://${VM1_IP}:${SPARK_MASTER_WEBUI_PORT}/json/" "$N_NODOS" <<'EOF' || FALLAS=$((FALLAS+1))
import json, sys, urllib.request
d = json.load(urllib.request.urlopen(sys.argv[1], timeout=10))
vivos = [w for w in d["workers"] if w["state"] == "ALIVE"]
for w in d["workers"]:
    print(f"   {w['host']:>15}  {w['state']:<6} cores={w['cores']:>2}  memoria={w['memory']/1024:.1f} GB")
print(f"   Total: {d['cores']} cores, {d['memory']/1024:.1f} GB, workers vivos={len(vivos)}")
sys.exit(0 if len(vivos) == int(sys.argv[2]) else 1)
EOF

if [ "$USE_HDFS" = "true" ]; then
  info "2. HDFS"
  VIVOS=$(como_bigdata "${HADOOP_HOME}/bin/hdfs dfsadmin -report" | grep -oE 'Live datanodes \([0-9]+\)' | grep -oE '[0-9]+' || echo 0)
  echo "   DataNodes vivos: ${VIVOS}"
  [ "$VIVOS" = "$N_NODOS" ] || FALLAS=$((FALLAS+1))
  como_bigdata "${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /tmp/prueba_cluster"
  como_bigdata "seq 1 2000000 | ${HADOOP_HOME}/bin/hdfs dfs -put -f - /tmp/prueba_cluster/numeros.txt"
  como_bigdata "${HADOOP_HOME}/bin/hdfs fsck /tmp/prueba_cluster/numeros.txt -files -blocks -locations" \
    | grep -E 'repl=|Average block replication' | sed 's/^/   /'
  ENTRADA="hdfs://${VM1_IP}:${HDFS_NN_PORT}/tmp/prueba_cluster/numeros.txt"
else
  ENTRADA=""
fi

info "3. Job distribuido de PySpark"
cat > /tmp/nyc_prueba_cluster.py <<'EOF'
import os, socket, sys, time
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("prueba-cluster").getOrCreate()
sc = spark.sparkContext
t0 = time.time()
# 200 tareas: cada una reporta en qué máquina se ejecutó. Se usa SPARK_LOCAL_IP
# (heredada de spark-env.sh) porque las VMs podrían tener el mismo hostname.
hosts = (sc.parallelize(range(200), 200)
           .map(lambda _: f"{socket.gethostname()} ({os.environ.get('SPARK_LOCAL_IP', '?')})")
           .countByValue())
print("   Tareas por máquina:", dict(hosts))
if len(sys.argv) > 2 and sys.argv[2]:
    n = spark.read.text(sys.argv[2]).count()
    print(f"   Líneas leídas desde HDFS: {n:,}")
print(f"   Tiempo: {time.time()-t0:.1f} s  |  executors: {sc._jsc.sc().getExecutorMemoryStatus().size() - 1}")
spark.stop()
sys.exit(0 if len(hosts) == int(sys.argv[1]) else 1)
EOF
chmod 644 /tmp/nyc_prueba_cluster.py
# Los mensajes internos de Spark van a un log; si el job falla se muestra la causa.
LOG_JOB=/tmp/nyc_prueba_cluster.log
if ! como_bigdata "${SPARK_HOME}/bin/spark-submit /tmp/nyc_prueba_cluster.py ${N_NODOS} '${ENTRADA}' 2>${LOG_JOB}"; then
  FALLAS=$((FALLAS+1))
  aviso "El job falló. Causa según Spark (${LOG_JOB}):"
  grep -E "ERROR|killed|Reason|unresponsive|refused|Exception" "$LOG_JOB" | grep -v "^\s*at " | head -12
fi

[ "$USE_HDFS" = "true" ] && como_bigdata "${HADOOP_HOME}/bin/hdfs dfs -rm -r -skipTrash /tmp/prueba_cluster" >/dev/null

echo
if [ "$FALLAS" -eq 0 ]; then
  ok "Cluster distribuido funcionando en las ${N_NODOS} VMs."
else
  error "${FALLAS} verificación(es) fallaron. Revisar: journalctl -u nyc-spark-worker -n 50 (en cada VM)."
fi
