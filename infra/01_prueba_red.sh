#!/usr/bin/env bash
# 01_prueba_red.sh: prueba si las VMs se pueden comunicar entre sí.
# Hay que correrlo antes de montar el cluster.
#
# Son tres pruebas:
#   1. ping, para ver si hay ruta de red entre las VMs.
#   2. conexión TCP a un puerto de prueba, para ver si el firewall deja pasar.
#   3. lo mismo entre todos los pares y en los dos sentidos, porque los workers no
#      solo hablan con el master y el driver (VM1), también entre ellos en el shuffle.
#      Con 3 VMs son 6 combinaciones: 1->2, 2->1, 1->3, 3->1, 2->3, 3->2.
#
# Uso, con una terminal abierta en cada VM:
#   en la VM que escucha:  bash infra/01_prueba_red.sh escuchar 7077
#   en la otra:            bash infra/01_prueba_red.sh probar <IP_de_la_otra> 7077
#   y después al revés.
#
# Si firewalld tiene el puerto cerrado, el modo "escuchar" pregunta si abrirlo por
# 15 minutos. Es temporal, no cambia la configuración permanente.
set -euo pipefail

PY=$(command -v python3 || echo /usr/libexec/platform-python)

uso() {
  echo "Uso:"
  echo "  en la VM que escucha:  bash infra/01_prueba_red.sh escuchar 7077"
  echo "  en la otra:            bash infra/01_prueba_red.sh probar <IP_de_la_otra> 7077"
  exit 1
}

escuchar() {
  local puerto=$1
  echo "IPs de esta VM (compártanlas con la otra):"
  ip -4 -o addr show scope global 2>/dev/null | awk '{print "  " $2 ": " $4}' || hostname -I

  if systemctl is-active --quiet firewalld 2>/dev/null; then
    if ! sudo firewall-cmd --query-port="${puerto}/tcp" >/dev/null 2>&1; then
      echo
      echo "firewalld está activo y el puerto ${puerto}/tcp está cerrado."
      read -r -p "¿Abrirlo por 15 minutos? (es temporal, no queda permanente) [s/N] " r
      if [[ "$r" =~ ^[sS]$ ]]; then
        sudo firewall-cmd --add-port="${puerto}/tcp" --timeout=15m
        echo "Abierto temporalmente. Se cerrará solo."
      else
        echo "Sin abrir: si la prueba falla, probablemente sea por el firewall."
      fi
    fi
  fi

  echo
  echo "Escuchando en 0.0.0.0:${puerto} ... (Ctrl+C para terminar; se apaga solo a los 10 minutos)"
  "$PY" - "$puerto" <<'EOF'
import socket, sys
puerto = int(sys.argv[1])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", puerto))
s.listen(5)
# Timeout para que no se quede ocupando el puerto (que luego usa Spark) si alguien lo deja corriendo.
s.settimeout(600)
while True:
    try:
        conn, addr = s.accept()
    except socket.timeout:
        print("  10 minutos sin conexiones: se libera el puerto y termina.", flush=True)
        break
    print(f"  Conexión recibida desde {addr[0]}:{addr[1]}  -> la red y el firewall permiten el tráfico", flush=True)
    conn.sendall(f"hola desde {socket.gethostname()}\n".encode())
    conn.close()
EOF
}

probar() {
  local ip=$1 puerto=$2
  echo "== 1. ping a ${ip}"
  if ping -c 3 -W 2 "$ip"; then
    echo "   OK: hay ruta de red."
  else
    echo "   ping falló. Puede ser que ICMP esté bloqueado; seguimos con TCP, que es lo que importa."
  fi

  echo
  echo "== 2. TCP a ${ip}:${puerto}"
  "$PY" - "$ip" "$puerto" <<'EOF'
import socket, sys, time
ip, puerto = sys.argv[1], int(sys.argv[2])
t0 = time.time()
try:
    with socket.create_connection((ip, puerto), timeout=5) as s:
        s.settimeout(5)
        resp = s.recv(100).decode(errors="replace").strip()
        print(f"   OK en {1000*(time.time()-t0):.1f} ms. Respuesta: '{resp}'")
except ConnectionRefusedError:
    print("   RECHAZADA: hay ruta, pero nadie escucha en ese puerto (¿se lanzó el modo 'escuchar'?).")
    sys.exit(2)
except socket.timeout:
    print("   TIMEOUT: casi seguro un firewall (de la VM o de la red universitaria) descarta los paquetes.")
    sys.exit(3)
except OSError as e:
    print(f"   ERROR: {e}  (sin ruta: las VMs pueden estar en redes aisladas)")
    sys.exit(4)
EOF

  echo
  echo "== 3. Latencia y ruta"
  command -v tracepath >/dev/null && tracepath -n -m 5 "$ip" 2>/dev/null | tail -3 || true
}

case "${1:-}" in
  escuchar) [ $# -eq 2 ] || uso; escuchar "$2" ;;
  probar)   [ $# -eq 3 ] || uso; probar "$2" "$3" ;;
  *) uso ;;
esac
