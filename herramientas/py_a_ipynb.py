"""Convierte un script con celdas '# %%' (formato percent) en un notebook .ipynb sin ejecutar.
Uso: python herramientas/py_a_ipynb.py fuente.py destino.ipynb
Las celdas '# %% [markdown]' llevan cada línea comentada con '# '."""
import json
import sys


def convertir(texto):
    celdas, tipo, lineas = [], None, []

    def cerrar():
        if tipo is None:
            return
        cuerpo = lineas[:]
        while cuerpo and not cuerpo[-1].strip():
            cuerpo.pop()
        while cuerpo and not cuerpo[0].strip():
            cuerpo.pop(0)
        if tipo == "markdown":
            cuerpo = [l[2:] if l.startswith("# ") else l.lstrip("#") for l in cuerpo]
        fuente = [l + "\n" for l in cuerpo]
        if fuente:
            fuente[-1] = fuente[-1].rstrip("\n")
        celda = {"cell_type": tipo, "metadata": {}, "source": fuente}
        if tipo == "code":
            celda.update({"execution_count": None, "outputs": []})
        celdas.append(celda)

    for l in texto.splitlines():
        if l.startswith("# %%"):
            cerrar()
            tipo, lineas = ("markdown" if "[markdown]" in l else "code"), []
        elif tipo is not None:
            lineas.append(l)
    cerrar()
    return {"cells": celdas, "metadata": {"kernelspec": {"display_name": "Python 3 (nyc-venv)", "language": "python", "name": "python3"},
            "language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 4}


if __name__ == "__main__":
    nb = convertir(open(sys.argv[1], encoding="utf-8").read())
    json.dump(nb, open(sys.argv[2], "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"{sys.argv[2]}: {len(nb['cells'])} celdas")
