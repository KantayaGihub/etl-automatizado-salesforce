from _carga_segura import preparar
import os
from pathlib import Path

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce


# ================================================================
# 1 Variables de entorno
# ================================================================
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")

if not SF_USERNAME or not SF_PASSWORD or not SF_SECURITY_TOKEN:
    raise ValueError(
        "Faltan variables de entorno. Verifica SF_USERNAME, SF_PASSWORD y SF_SECURITY_TOKEN."
    )

# ================================================================
# 1 Conexión a Salesforce
# ================================================================
sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain="login"
)


OBJECT_NAME = "asistencia_actividades_vivenciales__c"

# ==========================================================
# ==========================================================


print("=== Preparando registros para Salesforce ===")

# ==========================================================
# 3 Leer consolidado
# ==========================================================
input_path = Path(
    "data/consolidated/asistencia_actividades_vivenciales/BD_asistencia_actividades_vivenciales.csv"
)

if not input_path.exists():
    raise FileNotFoundError(f"No existe el consolidado: {input_path}")

df = pd.read_csv(
    input_path,
    dtype={"DNI": str}
)

print(f"Archivo leido correctamente: {len(df)} filas")

# ==========================================================
# 4 Convertir columnas
# ==========================================================
if "FECHA" in df.columns:
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce", dayfirst=True)
    df["FECHA"] = df["FECHA"].apply(
        lambda x: x.strftime("%Y-%m-%d") if pd.notnull(x) else None
    )

for col in ["ASISTENCIAS PROGRAMADAS", "ASISTENCIAS REALES", "% PART."]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ==========================================================
# 5 Renombrar columnas para Salesforce
# ==========================================================
mapeo = {
    "FECHA": "FECHA__c",
    "VALOR": "VALOR__c",
    "DNI": "DNI__c",
    "GRADO": "GRADO__c",
    "ASISTENCIAS PROGRAMADAS": "ASISTENCIAS_PROGRAMADAS__c",
    "MES": "MES__c",
    "ANIO_FUENTE": "ANIO_FUENTE__c",
    "ASISTENCIAS REALES": "ASISTENCIAS_REALES__c",
    "ACTIVIDAD": "ACTIVIDAD__c",
    "ESTADO": "ESTADO__c",
    "CONDICIÓN DE PARTICIPACIÓN": "CONDICIN_DE_PARTICIPACIN__c",
    "SEXO": "SEXO__c",
    "NOMBRES Y APELLIDOS": "NOMBRES_Y_APELLIDOS__c",
    "CENTRO": "CENTRO__c",
    "% PART.": "PART__c",
}

df = df.rename(columns=mapeo)

print("Columnas renombradas para Salesforce")

# ==========================================================
# 6 Limpiar nulos
# ==========================================================
df = df.where(pd.notnull(df), None)
df = df.replace({
    float("nan"): None,
    pd.NA: None,
    np.nan: None
})

# ==========================================================
# 7 Guardar debug
# ==========================================================
debug_path = Path(
    "data/consolidated/asistencia_actividades_vivenciales/DEBUG_para_salesforce.csv"
)
debug_path.parent.mkdir(parents=True, exist_ok=True)

df.to_csv(debug_path, index=False, encoding="utf-8-sig")
print(f"Archivo final guardado en: {debug_path}")

# ==========================================================
# 8 Convertir a records
# ==========================================================
preparar(sf, OBJECT_NAME, df)
