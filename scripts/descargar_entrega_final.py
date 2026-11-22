#!/usr/bin/env python3
"""
descargar_entrega_final.py: baja las fuentes de la entrega final a una carpeta local.

  - choques, vehiculos y personas: NYC Open Data, solo el periodo del proyecto, por semestres y
    en paralelo. El 28-oct-2026 la fuente se republicó con identificadores nuevos, por eso las
    tres tablas se bajan juntas y no se mezclan con los CSV de la primera entrega.
  - censo2020: población, área y punto interior de cada sector censal de los cinco condados. Sale
    del archivo geográfico del zip PL 94-171 (la API del censo exige clave).

Cada CSV se valida contra el conteo que da la propia API. Lo que ya se descargó y validó queda
en manifiesto.tsv y no se vuelve a pedir, así que si se corta basta con correrlo de nuevo.

Uso:  python descargar_entrega_final.py --salida /data/local/raw/entrega_final
"""
import argparse
import concurrent.futures as cf
import csv
import datetime as dt
import hashlib
import io
import os
import pathlib
import sys
import threading
import time
import zipfile

import requests

BASE = "https://data.cityofnewyork.us/resource"
FUENTES = {"choques": "h9gi-nx95", "vehiculos": "bm4k-52h4", "personas": "f55k-p6yu"}
URL_CENSO = ("https://www2.census.gov/programs-surveys/decennial/2020/data/"
             "01-Redistricting_File--PL_94-171/New_York/ny2020.pl.zip")
CONDADOS = {"005", "047", "061", "081", "085"}
# Totales oficiales del censo 2020 por condado
OFICIAL_2020 = {"005": 1_472_654, "047": 2_736_074, "061": 1_694_251, "081": 2_405_464, "085": 495_747}
# Posiciones en el archivo geográfico de PL 94-171 (documentación técnica, capítulo 2)
GEO_SUMLEV, GEO_GEOCODE, GEO_CONDADO, GEO_AREA_TIERRA, GEO_POBLACION, GEO_LAT, GEO_LON = 2, 9, 14, 84, 90, 92, 93
INTENTOS = 5

candado = threading.Lock()


def tramos(inicio, fin):
    """Semestres que cubren el periodo, sin traslapes."""
    salida = []
    for anio in range(inicio.year, fin.year + 1):
        for a, b in ((dt.date(anio, 1, 1), dt.date(anio, 6, 30)), (dt.date(anio, 7, 1), dt.date(anio, 12, 31))):
            a, b = max(a, inicio), min(b, fin)
            if a <= b:
                salida.append((a, b))
    return salida


def cabeceras():
    token = os.environ.get("SOCRATA_APP_TOKEN")     # solo se envía a NYC Open Data
    return {"X-App-Token": token} if token else {}


def contar_api(id_fuente, donde):
    r = requests.get(f"{BASE}/{id_fuente}.json", params={"$select": "count(*) as n", "$where": donde},
                     headers=cabeceras(), timeout=180)
    r.raise_for_status()
    return int(r.json()[0]["n"])


def sha256(ruta):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            h.update(bloque)
    return h.hexdigest()


def anotar(manifiesto, *campos):
    with candado, open(manifiesto, "a", encoding="utf-8") as f:
        f.write("\t".join(str(c) for c in campos) + "\n")


def ya_descargados(manifiesto):
    with open(manifiesto, encoding="utf-8") as f:
        return {fila["archivo"] for fila in csv.DictReader(f, delimiter="\t")}


def bajar_tramo(salida, manifiesto, fuente, desde, hasta):
    nombre = f"{fuente}/{fuente}_{desde:%Y%m%d}_{hasta:%Y%m%d}.csv"
    destino = salida / nombre
    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = salida / "_parciales" / destino.name
    parcial.parent.mkdir(parents=True, exist_ok=True)
    donde = f"crash_date between '{desde}T00:00:00' and '{hasta}T23:59:59'"
    for intento in range(1, INTENTOS + 1):
        try:
            esperadas = contar_api(FUENTES[fuente], donde)
            with requests.get(f"{BASE}/{FUENTES[fuente]}.csv", headers=cabeceras(), stream=True, timeout=300,
                              params={"$where": donde, "$limit": 5_000_000}) as r:
                r.raise_for_status()
                with open(parcial, "wb") as f:
                    for bloque in r.iter_content(1 << 18):
                        f.write(bloque)
            with open(parcial, newline="", encoding="utf-8") as f:
                filas = sum(1 for _ in csv.reader(f)) - 1
            if filas != esperadas:
                raise ValueError(f"llegaron {filas} filas y la API cuenta {esperadas}")
            parcial.replace(destino)
            anotar(manifiesto, nombre, FUENTES[fuente], dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                   destino.stat().st_size, filas, sha256(destino))
            print(f"[ OK ] {nombre}: {filas:,} filas", flush=True)
            return True
        except (requests.RequestException, ValueError, OSError) as e:
            print(f"[reintento {intento}/{INTENTOS}] {nombre}: {e}", flush=True)
            time.sleep(10 * intento)
    print(f"[FALLA] {nombre}", flush=True)
    return False


def sectores_del_censo(contenido_zip):
    """Una fila por sector censal (nivel de resumen 140) de los cinco condados de la ciudad."""
    with zipfile.ZipFile(contenido_zip) as z:
        with z.open("nygeo2020.pl") as f:
            for linea in io.TextIOWrapper(f, encoding="latin-1"):
                c = linea.rstrip("\r\n").split("|")
                if c[GEO_SUMLEV] == "140" and c[GEO_CONDADO] in CONDADOS:
                    yield {"geoid": c[GEO_GEOCODE], "county": c[GEO_CONDADO], "poblacion_2020": int(c[GEO_POBLACION]),
                           "lat": float(c[GEO_LAT]), "lon": float(c[GEO_LON]), "area_tierra_m2": int(c[GEO_AREA_TIERRA])}


def bajar_censo(salida, manifiesto):
    nombre = "censo2020/censo2020_sectores.csv"
    for intento in range(1, INTENTOS + 1):
        try:
            r = requests.get(URL_CENSO, timeout=600)
            r.raise_for_status()
            filas = list(sectores_del_censo(io.BytesIO(r.content)))
            totales = {}
            for f in filas:
                totales[f["county"]] = totales.get(f["county"], 0) + f["poblacion_2020"]
            if totales != OFICIAL_2020:
                raise ValueError(f"los condados no dan el total oficial: {totales}")
            destino = salida / nombre
            destino.parent.mkdir(parents=True, exist_ok=True)
            with open(destino, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(filas[0]))
                w.writeheader()
                w.writerows(filas)
            anotar(manifiesto, nombre, URL_CENSO, dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                   destino.stat().st_size, len(filas), sha256(destino))
            print(f"[ OK ] {nombre}: {len(filas):,} sectores, {sum(totales.values()):,} habitantes", flush=True)
            return True
        except (requests.RequestException, ValueError, OSError, zipfile.BadZipFile) as e:
            print(f"[reintento {intento}/{INTENTOS}] censo: {e}", flush=True)
            time.sleep(10 * intento)
    print("[FALLA] censo", flush=True)
    return False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--salida", required=True, type=pathlib.Path)
    ap.add_argument("--inicio", default="2019-01-01")
    ap.add_argument("--fin", default="2026-05-31")
    ap.add_argument("--hilos", type=int, default=8)
    ap.add_argument("--sin-censo", action="store_true")
    a = ap.parse_args()

    a.salida.mkdir(parents=True, exist_ok=True)
    manifiesto = a.salida / "manifiesto.tsv"
    if not manifiesto.exists():
        manifiesto.write_text("archivo\torigen\tdescargado_utc\tbytes\tfilas\tsha256\n", encoding="utf-8")
    listos = {n for n in ya_descargados(manifiesto) if (a.salida / n).exists()}

    t0 = time.time()
    trabajos = []
    with cf.ThreadPoolExecutor(a.hilos) as hilos:
        if not a.sin_censo and "censo2020/censo2020_sectores.csv" not in listos:
            trabajos.append(hilos.submit(bajar_censo, a.salida, manifiesto))
        # primero los semestres de personas, que son los más pesados
        for fuente in ("personas", "vehiculos", "choques"):
            for desde, hasta in tramos(dt.date.fromisoformat(a.inicio), dt.date.fromisoformat(a.fin)):
                if f"{fuente}/{fuente}_{desde:%Y%m%d}_{hasta:%Y%m%d}.csv" not in listos:
                    trabajos.append(hilos.submit(bajar_tramo, a.salida, manifiesto, fuente, desde, hasta))
        fallas = sum(not t.result() for t in trabajos)
    print(f"Descargas: {len(trabajos) - fallas} bien, {fallas} con falla, en {(time.time() - t0) / 60:.1f} min", flush=True)
    return 1 if fallas else 0


if __name__ == "__main__":
    sys.exit(main())
