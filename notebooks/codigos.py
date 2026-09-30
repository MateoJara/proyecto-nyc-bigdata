"""
Códigos de cada fuente y lo que significan.

Todo sale de la documentación oficial de cada dataset; al lado de cada diccionario
está de dónde lo sacamos. Los notebooks lo importan con `from codigos import ...`.
"""

# Pobreza: NYCgov Poverty Measure Data (cts7-vksw)
# Sacado del diccionario de datos que viene adjunto al dataset en NYC Open Data
# (NYCgov_Poverty_Measure_Data_Dictionary.xlsx), hoja "Column Info"
POBREZA_BORO = {1: "Bronx", 2: "Brooklyn", 3: "Manhattan", 4: "Queens", 5: "Staten Island"}

POBREZA_EDUCACION = {
    1: "Menos que secundaria",          # less than High School
    2: "Secundaria completa",           # High School Degree
    3: "Algo de universidad",           # Some College
    4: "Pregrado o más",                # Bachelors Degree or higher
}

# Ojo: acá 1 = en pobreza y 2 = no pobre (no es 0/1)
POBREZA_ESTADO = {1: "En pobreza", 2: "No en pobreza"}

POBREZA_EDAD = {1: "Menor de 18", 2: "18 a 64", 3: "65 o más"}

POBREZA_ETNIA = {
    1: "Blanco no hispano",
    2: "Negro no hispano",
    3: "Asiático no hispano",
    4: "Hispano (cualquier raza)",
    5: "Otro",
}

POBREZA_SEXO = {1: "Hombre", 2: "Mujer"}

# Pesos (hoja "Read Me", parte de Weighting): PWGTP para datos de personas y WGTP
# para datos de hogares. La muestra es de unas 60-70 mil personas y representa a todo
# NYC, menos a la gente que vive en alojamientos colectivos (group quarters).
PESO_PERSONA = "PWGTP"
PESO_HOGAR = "WGTP"

# Las 66 columnas que aparecen en el diccionario (juntando varios años). En el
# notebook de calidad se comparan con las que trae el archivo.
POBREZA_COLUMNAS_DICCIONARIO = [
    "AGEP", "AgeCateg", "Boro", "CIT", "CitizenStatus", "DIS", "DS", "ENG", "ESR", "EST_Childcare",
    "EST_Commuting", "EST_FICAtax", "EST_HEAP", "EST_Housing", "EST_HousingStatus", "EST_IncomeTax",
    "EST_MOOP", "EST_Nutrition", "EST_PovGap", "EST_PovGapIndex", "EducAttain", "Ethnicity", "FTPTWork",
    "FamType_PU", "HHT", "INTP_adj", "JWTR", "JWTRNS", "LANX", "MAR", "MRGP_adj", "MSP", "NP",
    "NYCgov_Income", "NYCgov_Pov_Stat", "NYCgov_REL", "NYCgov_Threshold", "OI_adj", "Off_Pov_Stat",
    "Off_Threshold", "PA_adj", "PWGTP", "Povunit_ID", "Povunit_Rel", "PreTaxIncome_PU", "REL", "RELP",
    "RELSHIPP", "RETP_adj", "RNTP_adj", "SCH", "SCHG", "SCHL", "SEMP_adj", "SERIALNO", "SEX", "SPORDER",
    "SSIP_adj", "SSP_adj", "TEN", "TotalWorkHrs_PU", "WAGP_adj", "WGTP", "WKHP", "WKW", "WKWN",
]

# Arrestos: NYPD Arrests Data (8h9b-rp9u y uip8-fykc)
# La descripción de la columna en los metadatos de NYC Open Data (/api/views/8h9b-rp9u.json) dice
# "B(Bronx), S(Staten Island), K(Brooklyn), M(Manhattan), Q(Queens)".
ARRESTOS_BORO = {"B": "Bronx", "K": "Brooklyn", "M": "Manhattan", "Q": "Queens", "S": "Staten Island"}

# LAW_CAT_CD: la documentación solo explica F, M y V (felony, misdemeanor, violation).
# Los demás que aparecen, como "I" o "9", no están documentados, así que quedan como
# "Otro / no documentado" y se cuentan en el reporte de calidad.
ARRESTOS_GRAVEDAD = {"F": "Felonía", "M": "Delito menor", "V": "Infracción (violation)"}

# JURISDICTION_CODE: 0 patrullaje, 1 transporte y 2 vivienda pública son del NYPD;
# de 3 en adelante son otras jurisdicciones.
ARRESTOS_JURISDICCION = {0: "NYPD - Patrullaje", 1: "NYPD - Transporte (metro)", 2: "NYPD - Vivienda pública"}

# Grupos de edad que publica el NYPD. Lo que no esté en esta lista (como "895") es inválido.
ARRESTOS_GRUPOS_EDAD = ["<18", "18-24", "25-44", "45-64", "65+"]

# dayofweek() de Spark da 1 = domingo hasta 7 = sábado
DIAS_SEMANA = {1: "Domingo", 2: "Lunes", 3: "Martes", 4: "Miércoles", 5: "Jueves", 6: "Viernes", 7: "Sábado"}
ORDEN_DIAS = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

# SAT 2012 (f9bf-2cp4): la tercera letra del DBN es el borough ("01M292" es de
# Manhattan). En 04_calidad se revisa que todas las letras sean válidas.
SAT_BORO_DBN = {"X": "Bronx", "K": "Brooklyn", "M": "Manhattan", "Q": "Queens", "R": "Staten Island"}
