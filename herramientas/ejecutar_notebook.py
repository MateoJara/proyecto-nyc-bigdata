"""Ejecuta un notebook y guarda la copia ejecutada también si una celda falla, para ver el error.
Uso: python herramientas/ejecutar_notebook.py origen.ipynb destino.ipynb"""
import pathlib
import sys

import nbformat
from nbclient import NotebookClient

origen, destino = map(pathlib.Path, sys.argv[1:3])
nb = nbformat.read(origen, as_version=4)
cliente = NotebookClient(nb, timeout=None, kernel_name="python3", resources={"metadata": {"path": str(origen.parent)}})
try:
    cliente.execute()
finally:
    nbformat.write(nb, destino)
print(f"Notebook ejecutado: {destino}")
