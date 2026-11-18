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


OBJECT_NAME = "BD_Habilidades_e__c"

# ==========================================================
# ==========================================================


print("=== CARGANDO CONSOLIDADO ===")

# ==========================================================
# 3 LEER CONSOLIDADO
# ==========================================================
INPUT_PATH = Path("data/consolidated/habilidades/BD_impacto_habilidades.csv")

if not INPUT_PATH.exists():
    raise FileNotFoundError(f"No existe el archivo: {INPUT_PATH}")

df = pd.read_csv(INPUT_PATH, dtype={"DNI__c": str})

print(f"Registros leidos: {len(df)}")


# ==========================================================
# 4 RENOMBRE PARA SALESFORCE
# ==========================================================
# (tu transformación ya viene bastante alineada)
mapeo = {
    "DNI__c": "DNI__c",
    "Apellidos_y_nombres__c": "Apellidos_y_nombres__c",
    "Grado__c": "Grado__c",
    "Sexo__c": "Sexo__c",
    "Centro__c": "Centro__c",
    "Permanencia__c": "Permanencia__c",
    "Condicin_actual__c": "Condicin_actual__c",
    "Habilidades__c": "Habilidades__c",
    "Nivel_de_logro__c": "Nivel_de_logro__c",
    "Evaluacion__c": "Evaluacion__c",
}

# Solo mantener columnas que Salesforce espera
df = df[list(mapeo.keys())]


# ==========================================================
# 5 LIMPIAR NULOS
# ==========================================================
df = df.where(pd.notnull(df), None)
df = df.replace({
    float('nan'): None,
    pd.NA: None,
    np.nan: None
})


debug_path = Path("data/consolidated/habilidades/DEBUG_para_salesforce.csv")
debug_path.parent.mkdir(parents=True, exist_ok=True)

df.to_csv(debug_path, index=False, encoding="utf-8-sig")
print(f"Archivo final guardado en: {debug_path}")


# ==========================================================
# 7 CONVERTIR A RECORDS
# ==========================================================
preparar(sf, OBJECT_NAME, df)
