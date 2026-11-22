#!/usr/bin/env python3
"""
extraer_clima.py: baja el clima por hora de Nueva York para el periodo del proyecto.

Fuentes, en este orden:
  1. OpenWeather (la API del enunciado). Se prueba cada producto con la
     clave del archivo .env (OPENWEATHER_API_KEY) y se guarda todo lo que la clave
     permita: clima actual, pronóstico de 5 días (sirve para mostrar cómo se usaría el
     modelo en la operación), histórico por hora y estadísticas. El estado de cada
     producto (código HTTP y mensaje) queda en openweather_estado.json.
  2. Open-Meteo, archivo histórico (reanálisis, sin clave). Cubre todas las horas
     de 2019 a 2026 y es la fuente que usa el modelo cuando OpenWeather no da el
     histórico completo. Se comparan ambas en las horas que coincidan (notebook 07).

Uso (en VM1, que tiene salida a internet):
    python3 scripts/extraer_clima.py --salida /data/local/raw/entrega_final/clima
    python3 scripts/extraer_clima.py --salida /data/local/raw/clima --inicio 2019-01-01 --fin 2026-05-31

Cada respuesta se guarda tal cual llega (JSON) en la capa cruda. Si un archivo ya
existe no se vuelve a pedir, así que se puede correr de nuevo sin gastar llamadas.
La clave nunca se escribe en los archivos ni en el registro.
"""
import argparse
import datetime as dt
import json
import os
import pathlib
import sys
import time

import requests

# Estación de referencia: Central Park (la misma que usa el Servicio Meteorológico para NYC)
LAT, LON = 40.7789, -73.9692
VARIABLES_OPEN_METEO = [
    "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
    "precipitation", "rain", "snowfall", "snow_depth", "weather_code",
    "cloud_cover", "wind_speed_10m", "wind_gusts_10m",
]


def leer_env(raiz):
    """Lee .env (KEY=VALOR por línea) si existe; las variables de entorno tienen prioridad."""
    env = {}
    archivo = raiz / ".env"
    if archivo.exists():
        for linea in archivo.read_text().splitlines():
            linea = linea.strip()
            if linea and not linea.startswith("#") and "=" in linea:
                k, v = linea.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    env.update({k: v for k, v in os.environ.items() if k.startswith("OPENWEATHER")})
    return env


def ocultar(valor, clave):
    """Quita la clave de cualquier texto o JSON antes de imprimirlo o guardarlo: los errores de
    requests traen la URL completa, con appid incluido."""
    if not clave:
        return valor
    if isinstance(valor, str):
        return valor.replace(clave, "<oculta>")
    if isinstance(valor, dict):
        return {k: ocultar(v, clave) for k, v in valor.items()}
    if isinstance(valor, list):
        return [ocultar(v, clave) for v in valor]
    return valor


def pedir(url, params, intentos=4, espera=10):
    """GET con reintentos para errores de red y 429/5xx. Devuelve (código, json o texto) sin la clave."""
    clave = params.get("appid")
    for i in range(intentos):
        try:
            r = requests.get(url, params=params, timeout=60)
        except requests.RequestException as e:
            if i == intentos - 1:
                return None, ocultar(str(e), clave)
            time.sleep(espera * (i + 1))
            continue
        if r.status_code in (429, 500, 502, 503, 504) and i < intentos - 1:
            time.sleep(espera * (i + 1))
            continue
        try:
            return r.status_code, ocultar(r.json(), clave)
        except ValueError:
            return r.status_code, ocultar(r.text, clave)[:500]
    return None, "sin respuesta"


def guardar(ruta, datos):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    tmp = ruta.with_suffix(ruta.suffix + ".part")
    tmp.write_text(json.dumps(datos, ensure_ascii=False))
    tmp.replace(ruta)


def open_meteo(salida, inicio, fin):
    """Un archivo por año, en UTC. No se pide en hora de Nueva York porque la API aplica un solo
    desfase a toda la serie y el horario de verano queda corrido una hora; la conversión se hace
    en el notebook 07. Devuelve los años que fallaron."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    fin_utc = fin + dt.timedelta(days=1)         # las últimas horas locales caen al día siguiente en UTC
    fallidos = []
    for anio in range(inicio.year, fin_utc.year + 1):
        desde = max(inicio, dt.date(anio, 1, 1))
        hasta = min(fin_utc, dt.date(anio, 12, 31))
        ruta = salida / "open_meteo" / f"open_meteo_{anio}.json"
        if ruta.exists():
            print(f"[open-meteo] {anio}: ya existe, se omite")
            continue
        params = {"latitude": LAT, "longitude": LON, "start_date": str(desde), "end_date": str(hasta),
                  "hourly": ",".join(VARIABLES_OPEN_METEO), "timezone": "GMT"}
        codigo, datos = pedir(url, params, intentos=5, espera=20)
        if codigo != 200 or not isinstance(datos, dict) or "hourly" not in datos:
            print(f"[open-meteo] {anio}: FALLA ({codigo}) {str(datos)[:200]}")
            fallidos.append(anio)
            continue
        guardar(ruta, datos)
        print(f"[open-meteo] {anio}: {len(datos['hourly']['time']):,} horas")
        time.sleep(2)                            # cortesía con la API gratuita
    return fallidos


def openweather(salida, clave, inicio, fin):
    estado = {"consultado_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
              "productos": {}}
    base = "https://api.openweathermap.org/data/2.5"
    historia = "https://history.openweathermap.org/data/2.5"
    comunes = {"lat": LAT, "lon": LON, "appid": clave, "units": "metric"}

    def anotar(nombre, codigo, datos):
        mensaje = datos.get("message") if isinstance(datos, dict) else str(datos)[:200]
        estado["productos"][nombre] = {"http": codigo, "mensaje": mensaje}
        print(f"[openweather] {nombre}: HTTP {codigo} {mensaje or ''}")
        return codigo == 200

    # 1. Clima actual y 2. pronóstico de 5 días cada 3 horas (plan gratuito)
    sello = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M")
    for nombre, url in [("actual", f"{base}/weather"), ("pronostico_5_dias", f"{base}/forecast")]:
        codigo, datos = pedir(url, comunes)
        if anotar(nombre, codigo, datos):
            guardar(salida / "openweather" / nombre / f"{nombre}_{sello}.json", datos)

    # 3. Histórico por hora (planes pagos o iniciativa de estudiantes): una semana por llamada
    semana = dt.timedelta(days=7)
    t = dt.datetime.combine(inicio, dt.time(0, 0), tzinfo=dt.timezone.utc)
    t_fin = dt.datetime.combine(fin, dt.time(23, 0), tzinfo=dt.timezone.utc)
    llamadas = guardadas = 0
    while t <= t_fin:
        ruta = salida / "openweather" / "historico" / f"historico_{t:%Y%m%d}.json"
        if not ruta.exists():
            params = {**comunes, "type": "hour", "start": int(t.timestamp()),
                      "end": int(min(t + semana, t_fin).timestamp())}
            codigo, datos = pedir(f"{historia}/history/city", params)
            llamadas += 1
            if codigo != 200:
                anotar("historico_por_hora", codigo, datos)
                break                            # sin acceso: no tiene sentido seguir pidiendo
            guardar(ruta, datos)
            guardadas += 1
            time.sleep(1.1)                      # límite de 60 llamadas por minuto
        t += semana
    else:
        estado["productos"]["historico_por_hora"] = {"http": 200, "mensaje": f"{guardadas} semanas nuevas"}
    print(f"[openweather] histórico: {llamadas} llamadas, {guardadas} semanas guardadas")

    # 4. Estadísticas climáticas del año (Statistical Weather Data API)
    codigo, datos = pedir(f"{historia}/aggregated/year", comunes)
    if anotar("estadisticas_anuales", codigo, datos):
        guardar(salida / "openweather" / "estadisticas" / "agregado_anual.json", datos)

    guardar(salida / "openweather" / "openweather_estado.json", estado)


def main():
    raiz = pathlib.Path(__file__).resolve().parents[1]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--salida", required=True, type=pathlib.Path)
    ap.add_argument("--inicio", default="2019-01-01")
    ap.add_argument("--fin", default="2026-05-31")
    ap.add_argument("--solo", choices=["openweather", "open-meteo"], help="correr una sola fuente")
    a = ap.parse_args()
    inicio, fin = dt.date.fromisoformat(a.inicio), dt.date.fromisoformat(a.fin)

    if a.solo in (None, "openweather"):
        clave = leer_env(raiz).get("OPENWEATHER_API_KEY")
        if clave:
            openweather(a.salida, clave, inicio, fin)
        else:
            print("[openweather] no hay OPENWEATHER_API_KEY en .env: se omite", file=sys.stderr)
    if a.solo in (None, "open-meteo"):
        fallidos = open_meteo(a.salida, inicio, fin)
        if fallidos:
            print(f"[open-meteo] faltan los años {fallidos}: volver a ejecutar", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
