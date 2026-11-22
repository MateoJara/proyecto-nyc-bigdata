"""Prueba el flujo de scripts/extraer_clima.py sin internet: se reemplaza requests.get por
respuestas simuladas (Open-Meteo responde bien; OpenWeather da 401 en el histórico)."""
import datetime as dt
import importlib.util
import json
import pathlib
import sys
import tempfile

raiz = pathlib.Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("extraer_clima", raiz / "scripts" / "extraer_clima.py")
ec = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ec)


class Resp:
    def __init__(self, codigo, datos):
        self.status_code, self._d, self.text = codigo, datos, json.dumps(datos)

    def json(self):
        return self._d


llamadas = []


def falso_get(url, params=None, timeout=None):
    llamadas.append(url)
    if "open-meteo" in url:
        horas = [f"{params['start_date']}T{h:02d}:00" for h in range(24)]
        return Resp(200, {"hourly": {"time": horas, **{v: [0.0] * 24 for v in ec.VARIABLES_OPEN_METEO}}})
    if url.endswith("/weather"):
        return Resp(200, {"main": {"temp": 10}})
    if url.endswith("/forecast"):
        return Resp(200, {"list": [{"dt": 1, "main": {"temp": 5}, "weather": [{"description": "x"}]}]})
    return Resp(401, {"cod": 401, "message": "Invalid API key"})


ec.requests.get = falso_get
ec.time.sleep = lambda s: None
with tempfile.TemporaryDirectory() as d:
    salida = pathlib.Path(d)
    ec.openweather(salida, "CLAVE_DE_PRUEBA", dt.date(2024, 1, 1), dt.date(2024, 1, 31))
    ec.open_meteo(salida, dt.date(2024, 12, 30), dt.date(2025, 1, 2))
    estado = json.loads((salida / "openweather" / "openweather_estado.json").read_text())
    assert estado["productos"]["actual"]["http"] == 200
    assert estado["productos"]["historico_por_hora"]["http"] == 401
    assert sum("history/city" in u for u in llamadas) == 1, "debe parar al primer 401"
    assert sorted(p.name for p in (salida / "open_meteo").iterdir()) == ["open_meteo_2024.json", "open_meteo_2025.json"]
    assert "CLAVE_DE_PRUEBA" not in json.dumps(estado), "la clave no debe quedar escrita"
    n = len(llamadas)
    ec.open_meteo(salida, dt.date(2024, 12, 30), dt.date(2025, 1, 2))
    assert len(llamadas) == n, "no debe volver a pedir lo que ya está guardado"

# Un error de red trae la URL con la clave: no debe salir en el mensaje ni en el archivo de estado
def red_caida(url, params=None, timeout=None):
    raise ec.requests.ConnectionError(f"Max retries exceeded with url: {url}?appid={(params or {}).get('appid')}")


ec.requests.get = red_caida
with tempfile.TemporaryDirectory() as d:
    salida = pathlib.Path(d)
    ec.openweather(salida, "CLAVE_DE_PRUEBA", dt.date(2024, 1, 1), dt.date(2024, 1, 2))
    assert "CLAVE_DE_PRUEBA" not in (salida / "openweather" / "openweather_estado.json").read_text(), "clave en un error de red"
    assert ec.open_meteo(salida, dt.date(2024, 1, 1), dt.date(2024, 1, 2)) == [2024], "un año fallido se debe reportar"
print("extraer_clima.py: flujo OK (para en el 401, no repite descargas, no guarda la clave)")
