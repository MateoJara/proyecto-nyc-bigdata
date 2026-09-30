"""
Estilo de los gráficos. Cada notebook hace `from estilo import ...` para que todas las
figuras del documento se vean parecidas y cada borough tenga siempre el mismo color.
"""
import matplotlib as mpl
import matplotlib.pyplot as plt

# Colores para categorías, siempre en este orden. Escogidos para que se distingan
# bien entre sí, también para personas daltónicas.
CATEGORICA = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]

# Cada borough con su color fijo en todos los gráficos
BOROUGHS = ["Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"]
COLOR_BOROUGH = dict(zip(BOROUGHS, CATEGORICA))

# Para cantidades: el mismo azul, de claro a oscuro
SECUENCIAL = ["#dceafa", "#a9cbf2", "#6fa6e6", "#2a78d6", "#1b56a0", "#10366a"]

TINTA = "#0b0b0b"
TINTA_SECUNDARIA = "#52514e"
REJILLA = "#e4e3df"

mpl.rcParams.update({
    "figure.dpi": 110,
    "savefig.dpi": 200,
    "savefig.bbox": "tight",
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.edgecolor": REJILLA,
    "axes.labelcolor": TINTA_SECUNDARIA,
    "axes.titlesize": 12,
    "axes.titleweight": "bold",
    "axes.titlecolor": TINTA,
    "axes.titlelocation": "left",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.color": REJILLA,
    "grid.linewidth": 0.6,
    "axes.axisbelow": True,
    "axes.prop_cycle": mpl.cycler(color=CATEGORICA),
    "xtick.color": TINTA_SECUNDARIA,
    "ytick.color": TINTA_SECUNDARIA,
    "lines.linewidth": 2,
    "legend.frameon": False,
    "font.size": 10,
})


def miles(x, _pos=None):
    """Número con punto de miles, como se escribe en español (1234567 -> 1.234.567)."""
    return f"{x:,.0f}".replace(",", ".")


def guardar(fig, nombre):
    """Guarda la figura en ../figuras/<nombre>.png para usarla en el documento."""
    fig.savefig(f"../figuras/{nombre}.png")
