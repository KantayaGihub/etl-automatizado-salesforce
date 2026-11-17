from _carga_segura import preparar
import os

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce


print("================================================")
print("=== ETL PREPARACION SALESFORCE - ASISTENCIA REGULAR ===")
print("================================================")


OBJECT_NAME = "Asistencias_e7__c"
RUTA_CONSOLIDADO = "data/consolidated/asistencia_regular/asistencias_consolidado_kantaya.csv"


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


print("\n=== CONECTANDO A SALESFORCE ===")

sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain="login"
)


print("Conexion Salesforce OK")


print("\n=== CARGANDO CONSOLIDADO ===")

Asistencias = pd.read_csv(
    RUTA_CONSOLIDADO,
    sep=",",
    dtype={"DNI": str}
)

print(f"Archivo leido correctamente: {len(Asistencias)} filas")


print("\n=== TRANSFORMANDO FECHAS ===")

for col in ["F_INCORPORACION", "F_SALIDA", "FECHA"]:
    if col in Asistencias.columns:
        Asistencias[col] = pd.to_datetime(
            Asistencias[col], errors='coerce', dayfirst=True
        )
        Asistencias[col] = Asistencias[col].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None
        )
    else:
        print(f"Advertencia: No se encontro columna {col} en el consolidado")


print("\n=== TRANSFORMANDO NUMERICOS ===")

for col in ["A_ESPERADAS", "A_REALES", "PORC_PART"]:
    if col in Asistencias.columns:
        Asistencias[col] = pd.to_numeric(
            Asistencias[col], errors="coerce"
        )
    else:
        print(f"Advertencia: No se encontro columna {col} en el consolidado")


print("\n=== NORMALIZANDO NULOS ===")

Asistencias = Asistencias.where(pd.notnull(Asistencias), None)
Asistencias = Asistencias.replace({float('nan'): None, pd.NA: None, np.nan: None})


print("\n=== PREPARANDO COLUMNAS SALESFORCE ===")

Asistencias.columns = [col + "__c" for col in Asistencias.columns]

print(f"Campos finales: {len(Asistencias.columns)}")


preparar(sf, OBJECT_NAME, Asistencias)
