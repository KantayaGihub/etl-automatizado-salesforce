from _carga_segura import preparar
import os

import numpy as np
import pandas as pd

from simple_salesforce import Salesforce


print("====================================================")
print("=== ETL PREPARACION SALESFORCE - EXTRACURRICULARES ===")
print("====================================================")


# ================================================================
# CONFIGURACIÓN
# ================================================================
OBJECT_NAME = "Asistencias_Extracurriculares_e__c"

RUTA_CONSOLIDADO = (
    "data/consolidated/asistencias_extracurriculares/"
    "asistencias_extra_consolidado_kantaya.csv"
)


# ================================================================
# FUNCIONES AUXILIARES BULK API
# ================================================================


# ================================================================
# VARIABLES DE ENTORNO
# ================================================================
print("\n=== VALIDANDO VARIABLES DE ENTORNO ===")

SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")

if not SF_USERNAME:
    raise ValueError("Falta SF_USERNAME")

if not SF_PASSWORD:
    raise ValueError("Falta SF_PASSWORD")

if not SF_SECURITY_TOKEN:
    raise ValueError("Falta SF_SECURITY_TOKEN")

print("Variables cargadas correctamente")


# ================================================================
# CONEXIÓN SALESFORCE  —  UNA SOLA INSTANCIA para todo el script
# ================================================================
print("\n=== CONECTANDO A SALESFORCE ===")

sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain="login"
)


print("Conexión Salesforce OK")


# ================================================================
# ================================================================


# ================================================================
# CARGAR CONSOLIDADO
# ================================================================
print("\n=== CARGANDO CONSOLIDADO ===")

Asistencias = pd.read_csv(
    RUTA_CONSOLIDADO,
    sep=",",
    dtype={"DNI": str}
)

print("CSV cargado correctamente")
print(f"Filas: {len(Asistencias):,}")
print(f"Columnas: {len(Asistencias.columns)}")


# ================================================================
# VALIDACIONES QA
# ================================================================
print("\n=== VALIDACIONES QA ===")

dup_cols = [c for c in ["DNI", "FECHA"] if c in Asistencias.columns]

if dup_cols:

    dups = Asistencias.duplicated(subset=dup_cols).sum()
    print(f"Duplicados {dup_cols}: {dups:,}")

for c in ["DNI", "FECHA", "ASISTENCIA"]:

    if c in Asistencias.columns:

        nulos = Asistencias[c].isna().sum()
        print(f"Nulos {c}: {nulos:,} / {len(Asistencias):,}")

if "CENTRO" in Asistencias.columns:

    print("\n=== TOP CENTROS ===")
    print(Asistencias["CENTRO"].value_counts(dropna=False).head(10))


# ================================================================
# FECHAS
# ================================================================
print("\n=== TRANSFORMANDO FECHAS ===")

for col in ["F_INCORPORACION", "F_SALIDA", "FECHA"]:

    if col in Asistencias.columns:

        Asistencias[col] = pd.to_datetime(
            Asistencias[col],
            errors="coerce",
            dayfirst=True
        )

        Asistencias[col] = Asistencias[col].apply(
            lambda x: x.strftime("%Y-%m-%d") if pd.notnull(x) else None
        )

print("Fechas transformadas")


# ================================================================
# NUMÉRICOS
# ================================================================
print("\n=== TRANSFORMANDO NUMÉRICOS ===")

for col in ["A_ESPERADAS", "A_REALES", "PORC_PART"]:

    if col in Asistencias.columns:

        Asistencias[col] = pd.to_numeric(Asistencias[col], errors="coerce")

print("Numéricos transformados")


# ================================================================
# LIMPIEZA CENTRO
# ================================================================
print("\n=== LIMPIANDO CENTRO ===")

if "CENTRO" in Asistencias.columns:

    Asistencias["CENTRO"] = (
        Asistencias["CENTRO"]
        .astype(str)
        .str.replace(" REGISTRO DE ASISTENCIA 2025.xlsx", "", regex=False)
        .str.replace(" REGISTRO DE ASISTENCIA 2026.xlsx", "", regex=False)
        .str.replace("_", " ", regex=False)
    )

    Asistencias["CENTRO"] = Asistencias["CENTRO"].replace("nan", None)

print("Centro limpio")


# ================================================================
# NORMALIZAR NULOS
# ================================================================
print("\n=== NORMALIZANDO NULOS ===")

Asistencias = Asistencias.where(pd.notnull(Asistencias), None)

Asistencias = Asistencias.replace({
    float("nan"): None,
    pd.NA: None,
    np.nan: None
})

print("Nulos normalizados")


# ================================================================
# RENOMBRAR COLUMNAS
# ================================================================
print("\n=== PREPARANDO COLUMNAS SALESFORCE ===")

Asistencias.columns = [col + "__c" for col in Asistencias.columns]


# ================================================================
# VALIDAR CAMPOS SALESFORCE
# ================================================================
print("\n=== VALIDANDO CAMPOS SALESFORCE ===")

sf_fields = {
    f["name"]
    for f in sf.Asistencias_Extracurriculares_e__c.describe()["fields"]
}

invalid_cols = [c for c in Asistencias.columns if c not in sf_fields]

if invalid_cols:

    print("\n Campos inexistentes en Salesforce:")
    print(invalid_cols)
    raise ValueError("Hay campos sin correspondencia en Salesforce; corregir el mapeo antes de preparar")

print("Validación de campos OK")


# ================================================================
# CONVERTIR A RECORDS
# ================================================================
preparar(sf, OBJECT_NAME, Asistencias)
