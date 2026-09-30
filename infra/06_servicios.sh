#!/usr/bin/env bash
# 06_servicios.sh: registra cada proceso como servicio de systemd y lo arranca.
# Se corre en todas las VMs, empezando por VM1:   sudo bash infra/06_servicios.sh
#
# Usamos systemd en vez de start-all.sh / start-dfs.sh porque:
#   - esos scripts entran por SSH a cada nodo y necesitan llaves SSH sin contraseña
#     entre las VMs. Con systemd cada VM arranca sus propios procesos.
#   - systemd reinicia el proceso si se cae y lo vuelve a subir si se reinicia la VM.
#   - los logs quedan en journalctl -u <servicio>.
#
# Servicios:            VM1                          VM2 y VM3
#   nyc-hdfs-namenode   sí                           no
#   nyc-hdfs-datanode   sí                           sí
#   nyc-spark-master    sí                           no
#   nyc-spark-worker    sí                           sí
#   nyc-spark-history   sí                           no
#   nyc-jupyter         sí (solo en 127.0.0.1)       no
source "$(dirname "$0")/lib.sh"
validar_config
detectar_rol
[ "$(id -u)" -eq 0 ] || error "Ejecutar con sudo."

SPARK_HOME="${INSTALL_DIR}/spark"
HADOOP_HOME="${INSTALL_DIR}/hadoop"
JH=$(java_home)
UNIDADES=()

unidad() {  # unidad <nombre> <descripción> <ExecStart> [After extra] [Environment extra]
  local nombre=$1 desc=$2 exec=$3 after=${4:-} envextra=${5:-}
  cat > "/etc/systemd/system/${nombre}.service" <<EOF
# Generado por infra/06_servicios.sh
[Unit]
Description=${desc} (proyecto-nyc-bigdata)
After=network-online.target ${after}
Wants=network-online.target

[Service]
Type=simple
User=${CLUSTER_USER}
Group=${CLUSTER_USER}
Environment=JAVA_HOME=${JH}
Environment=SPARK_HOME=${SPARK_HOME}
Environment=HADOOP_HOME=${HADOOP_HOME}
Environment=SPARK_NO_DAEMONIZE=true
${envextra}
ExecStart=${exec}
Restart=on-failure
RestartSec=15
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
  UNIDADES+=("$nombre")
}

if [ "$USE_HDFS" = "true" ]; then
  [ "$ROL" = "vm1" ] && unidad nyc-hdfs-namenode "HDFS NameNode" "${HADOOP_HOME}/bin/hdfs namenode"
  unidad nyc-hdfs-datanode "HDFS DataNode" "${HADOOP_HOME}/bin/hdfs datanode"
fi

if [ "$ROL" = "vm1" ]; then
  unidad nyc-spark-master "Spark Master" "${SPARK_HOME}/sbin/start-master.sh"
  unidad nyc-spark-history "Spark History Server" "${SPARK_HOME}/sbin/start-history-server.sh"
  unidad nyc-spark-worker "Spark Worker" \
    "${SPARK_HOME}/sbin/start-worker.sh spark://${VM1_IP}:${SPARK_MASTER_PORT}" "nyc-spark-master.service"
  unidad nyc-jupyter "JupyterLab (driver de Spark)" \
    "${VENV_DIR}/bin/jupyter lab --ip=127.0.0.1 --port=${JUPYTER_PORT} --no-browser --ServerApp.root_dir=${REPO_DIR}" \
    "nyc-spark-master.service" \
    "Environment=PYSPARK_PYTHON=${VENV_DIR}/bin/python
Environment=HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
Environment=PATH=${VENV_DIR}/bin:${SPARK_HOME}/bin:/usr/local/bin:/usr/bin:/bin
WorkingDirectory=${REPO_DIR}"
else
  unidad nyc-spark-worker "Spark Worker" "${SPARK_HOME}/sbin/start-worker.sh spark://${VM1_IP}:${SPARK_MASTER_PORT}"
fi

if [ "$ROL" = "vm1" ] && [ ! -d "$REPO_DIR" ]; then
  aviso "No existe ${REPO_DIR} (lo crea 02_base.sh): Jupyter lo usa como carpeta raíz. Clonen el repo ahí:"
  aviso "  sudo -iu ${CLUSTER_USER} git clone https://github.com/MateoJara/proyecto-nyc-bigdata.git ${REPO_DIR}"
  install -d -o "$CLUSTER_USER" -g "$CLUSTER_USER" "$REPO_DIR"
fi

# Los puertos de Spark tienen que estar libres o tomados por el mismo Spark (java). Si otro
# programa tiene el 7077, los workers se conectan a ese y no al master.
PUERTOS_ROL="${SPARK_WORKER_PORT} ${SPARK_WORKER_WEBUI_PORT}"
[ "$ROL" = "vm1" ] && PUERTOS_ROL="${SPARK_MASTER_PORT} ${SPARK_MASTER_WEBUI_PORT} ${PUERTOS_ROL}"
for p in $PUERTOS_ROL; do
  ocupante=$(ss -ltnpH "sport = :${p}" 2>/dev/null | grep -oP 'users:\(\("\K[^"]+' | head -1)
  if [ -n "$ocupante" ] && [ "$ocupante" != "java" ]; then
    ss -ltnp "sport = :${p}"
    error "El puerto ${p} está ocupado por '${ocupante}'. Deténganlo antes de arrancar el cluster."
  fi
done

echo "Servicios a habilitar y arrancar en ${ROL}: ${UNIDADES[*]}"
confirmar "¿Continuar?"

systemctl daemon-reload
for u in "${UNIDADES[@]}"; do
  systemctl enable --now "$u"
done
sleep 8
for u in "${UNIDADES[@]}"; do
  printf '  %-22s %s\n' "$u" "$(systemctl is-active "$u")"
done

if [ "$ROL" = "vm1" ]; then
  echo
  info "Para ver las interfaces desde el portátil, abrir un túnel SSH:"
  echo "  ssh -L 8080:${VM1_IP}:8080 -L 4040:${VM1_IP}:4040 -L 9870:${VM1_IP}:9870 \\"
  echo "      -L 18080:${VM1_IP}:18080 -L 8888:127.0.0.1:8888 <usuario>@<IP_de_VM1>"
  info "Token de Jupyter:  sudo -iu ${CLUSTER_USER} ${VENV_DIR}/bin/jupyter server list"
fi
