"""Pruebas de las funciones de Python puro de notebooks/noche.py.
Se corren con:  python tests/probar_noche.py   (no necesitan Spark ni datos).
Si scikit-learn está instalado, además se comparan las métricas con las suyas."""
import datetime as dt
import pathlib
import sys

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "notebooks"))
import noche  # noqa: E402

rng = np.random.default_rng(0)

# --- calendario: fechas conocidas ---
cal = {(f["nombre"], f["fecha"].year): f["fecha"] for f in noche.calendario(2019, 2026)}
assert cal[("Acción de Gracias", 2025)] == dt.date(2025, 11, 27)
assert cal[("Memorial Day", 2024)] == dt.date(2024, 5, 27)
assert cal[("Martin Luther King", 2026)] == dt.date(2026, 1, 19)
assert cal[("Día del Trabajo", 2023)] == dt.date(2023, 9, 4)
assert cal[("Independencia", 2021)] == dt.date(2021, 7, 5)        # 4 de julio cayó domingo
assert cal[("Navidad", 2022)] == dt.date(2022, 12, 26)            # 25 cayó domingo
assert cal[("Víspera de Acción de Gracias", 2025)] == dt.date(2025, 11, 26)
assert ("Juneteenth", 2020) not in cal and cal[("Juneteenth", 2021)] == dt.date(2021, 6, 18)
print("calendario OK")

assert cal[("Víspera de Memorial Day", 2026)] == dt.date(2026, 5, 24)   # domingo

# --- clima: de UTC a hora de Nueva York ---
import pandas as pd  # noqa: E402

_utc = pd.date_range("2024-03-09", "2024-11-05", freq="h", inclusive="left")
_h, _faltan = noche.horas_locales(pd.DataFrame({"time": _utc.strftime("%Y-%m-%dT%H:%M"), "utc_h": _utc.hour}),
                                  "2024-03-10", "2024-11-03")
_h = _h.set_index("ts_local")
assert _faltan == 0
assert pd.Timestamp("2024-03-10 02:00") not in _h.index                     # hora que no existe
assert _h.loc[pd.Timestamp("2024-07-01 00:00"), "utc_h"] == 4               # verano: UTC-4
assert _h.loc[pd.Timestamp("2024-03-10 00:00"), "utc_h"] == 5               # invierno: UTC-5
assert _h.index.is_unique
print("horas locales OK")

# --- rangos y percentiles ---
assert np.allclose(noche.rangos([10, 20, 20, 5]), [2, 3.5, 3.5, 1])
assert np.allclose(noche.percentil([1, 2, 3]), [0, 0.5, 1])
print("rangos OK")

# --- captura en el top ---
y = np.array([1, 0, 0, 0, 1, 0, 0, 0, 0, 0])
p = np.array([.9, .8, .1, .1, .1, .1, .1, .1, .1, .1])
assert abs(noche.captura_top(y, p, 0.1) - 0.5) < 1e-12           # el top 1 tiene 1 de 2 positivos
assert abs(noche.captura_top(y, np.ones(10), 0.3) - 0.3) < 1e-12  # puntaje constante = fracción
print("captura OK")

# --- AUC y ARI contra scikit-learn ---
try:
    from sklearn.metrics import adjusted_rand_score, average_precision_score, roc_auc_score
    for _ in range(20):
        y = rng.integers(0, 2, 500)
        s = np.round(rng.normal(size=500) + y * 0.7, 1)               # con empates
        assert abs(noche.auc_roc(y, s) - roc_auc_score(y, s)) < 1e-9
        assert abs(noche.auc_pr(y, s) - average_precision_score(y, s)) < 1e-9
        a, b = rng.integers(0, 4, 300), rng.integers(0, 5, 300)
        assert abs(noche.indice_rand_ajustado(a, b) - adjusted_rand_score(a, b)) < 1e-9
    print("métricas iguales a scikit-learn OK")
except ImportError:
    print("scikit-learn no está instalado: se omite la comparación")

try:
    from scipy.stats import spearmanr
    x, z = rng.normal(size=50), rng.normal(size=50)
    assert abs(noche.spearman(x, z) - spearmanr(x, z).correlation) < 1e-9
    print("spearman igual a scipy OK")
except ImportError:
    print("scipy no está instalado: se omite la comparación de Spearman")
print("Todas las pruebas pasaron.")
