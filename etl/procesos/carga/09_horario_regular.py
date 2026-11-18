from _carga_segura import preparar
import os
from pathlib import Path

import pandas as pd
import numpy as np
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


OBJECT_NAME = "horario_regular__c"


# ==========================================================
# ==========================================================


print("=== CARGANDO CONSOLIDADO ===")

# ==========================================================
# 3 Leer consolidado
# ==========================================================
INPUT_PATH = Path("data/consolidated/horario_regular/base_horaria_final.csv")

if not INPUT_PATH.exists():
    raise FileNotFoundError(f"No existe el archivo: {INPUT_PATH}")

df = pd.read_csv(INPUT_PATH)

print(f"Registros leidos: {len(df)}")


# ==========================================================
# 4 Renombrar columnas → Salesforce
# ==========================================================
df.columns = [col + "__c" for col in df.columns]


# ==========================================================
# 5 Limpiar nulos
# ==========================================================
df = df.where(pd.notnull(df), None)
df = df.replace({
    float('nan'): None,
    pd.NA: None,
    np.nan: None
})


# ==========================================================
# 6 DEBUG (MUY IMPORTANTE)
# ==========================================================
debug_path = Path("data/consolidated/horario_regular/DEBUG_para_salesforce.csv")
debug_path.parent.mkdir(parents=True, exist_ok=True)

df.to_csv(debug_path, index=False, encoding="utf-8-sig")
print(f"Archivo final guardado en: {debug_path}")


# ==========================================================
# 7 Convertir a records
# ==========================================================
preparar(sf, OBJECT_NAME, df)
