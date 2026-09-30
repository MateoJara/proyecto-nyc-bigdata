#!/usr/bin/env bash
# 00_diagnostico.sh: revisa los recursos de una VM antes de montar el cluster.
# No instala ni cambia nada, solo lee información del sistema.
# Se corre en cada VM y se comparte la salida para decidir cuánto darle a cada worker:
#     bash infra/00_diagnostico.sh | tee diagnostico_$(hostname -s).txt
set -uo pipefail

titulo() { printf '\n==================== %s ====================\n' "$1"; }
existe() { command -v "$1" >/dev/null 2>&1; }

titulo "Identidad"
echo "Fecha:        $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "Hostname:     $(hostname)"
echo "FQDN:         $(hostname -f 2>/dev/null || echo 'n/d')"
echo "Usuario:      $(whoami)  (sudo: $(sudo -n true 2>/dev/null && echo 'sin contraseña' || echo 'pide contraseña (verificar con: sudo -v)'))"
[ -r /etc/os-release ] && . /etc/os-release && echo "Sistema:      ${PRETTY_NAME}"
echo "Kernel:       $(uname -r)  arquitectura: $(uname -m)"
echo "Virtualiz.:   $(systemd-detect-virt 2>/dev/null || echo 'n/d')"

titulo "CPU"
echo "nproc:        $(nproc)"
if existe lscpu; then
  lscpu | grep -E '^(Model name|Socket\(s\)|Core\(s\) per socket|Thread\(s\) per core|CPU MHz|Hypervisor vendor)' || true
fi

titulo "Memoria"
free -h
echo
echo "Carga actual: $(cut -d' ' -f1-3 /proc/loadavg)"

titulo "Disco"
df -hT -x tmpfs -x devtmpfs -x overlay 2>/dev/null || df -h
echo
echo "Punto de montaje con más espacio libre:"
df -P -x tmpfs -x devtmpfs 2>/dev/null | awk 'NR>1 {print $4, $6}' | sort -nr | head -1 \
  | awk '{printf "  %s  (%.1f GB libres)\n", $2, $1/1024/1024}'

titulo "Red"
echo "IPs (sin loopback):"
ip -4 -o addr show scope global 2>/dev/null | awk '{printf "  %-10s %s\n", $2, $4}'
echo "Ruta por defecto: $(ip route show default 2>/dev/null | head -1)"
echo "El hostname resuelve a: $(getent hosts "$(hostname)" | awk '{print $1}' | paste -sd' ')"
echo "  (si aparece 127.0.0.1 o ::1, Spark podría anunciarse con loopback: lo corregimos con SPARK_LOCAL_IP)"
echo "DNS: $(grep -E '^nameserver' /etc/resolv.conf 2>/dev/null | awk '{print $2}' | paste -sd' ')"
echo -n "Internet (HTTPS a archive.apache.org): "
if existe curl && curl -sS -m 10 -o /dev/null https://archive.apache.org/dist/spark/ 2>/dev/null; then echo OK; else echo FALLA; fi

titulo "Firewall y SELinux"
if existe firewall-cmd; then
  echo "firewalld: $(systemctl is-active firewalld 2>/dev/null)"
  sudo -n firewall-cmd --get-active-zones 2>/dev/null || echo "  (sin sudo no se pueden listar zonas)"
  sudo -n firewall-cmd --list-all 2>/dev/null || true
else
  echo "firewall-cmd no está instalado"
fi
echo "SELinux: $(getenforce 2>/dev/null || echo 'n/d')"

titulo "Puertos que usará el cluster (¿ya ocupados?)"
PUERTOS="7077 7078 8080 8081 4040 8888 9000 9864 9866 9867 9870 40000"
for p in $PUERTOS; do
  if ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${p}\$"; then
    echo "  $p  OCUPADO"
  else
    echo "  $p  libre"
  fi
done

titulo "Software instalado"
if existe java; then java -version 2>&1 | head -1 | sed 's/^/java:    /'; else echo "java:    no instalado"; fi
for py in python3 python3.10 python3.11 python3.12; do
  existe "$py" && echo "$py: $($py --version 2>&1)"
done
for t in git curl wget tar nc ncat docker podman; do
  printf '%-8s %s\n' "$t:" "$(existe "$t" && echo sí || echo no)"
done
echo "Repositorios dnf con python3.11 disponible: $(dnf -q list --available python3.11 2>/dev/null | grep -c python3.11 || echo 0)"

titulo "Sugerencia de dimensionamiento (preliminar)"
CORES=$(nproc)
MEM_MB=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
echo "Recursos: ${CORES} cores, ${MEM_MB} MB de RAM"
echo "Reglas que usamos (detalle en docs/arquitectura.md):"
echo "  - Reservar ~1,5 GB y 1 core para el sistema operativo."
echo "  - Reservar ~1,5 GB para los demonios de HDFS (NameNode/DataNode) si se usa HDFS."
echo "  - En VM1 reservar además memoria para el driver/Jupyter (2-4 GB) y 1 core."
echo "  El dimensionamiento final lo fijamos en infra/cluster.env con los datos de todas las VMs."
