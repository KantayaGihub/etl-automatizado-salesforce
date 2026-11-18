from _carga_segura import preparar
import os
import pandas as pd
import numpy as np
from simple_salesforce import Salesforce
from pathlib import Path


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


# ==========================================================
# ==========================================================


print("=== INICIANDO CARGA ===")

# ==========================================================
# 3 Leer consolidado
# ==========================================================
ruta_consolidado = Path(
    "data/consolidated/calificaciones_area_competencia/BD_Promedios_Areas_LB_LS_Consolidado.csv"
)

if not ruta_consolidado.exists():
    raise FileNotFoundError(f"No existe el consolidado: {ruta_consolidado}")

Calificaciones = pd.read_csv(ruta_consolidado)

print(f"Archivo leido correctamente: {len(Calificaciones)} filas")

# Las columnas ya incluyen el sufijo __c desde la consolidacion

# ==========================================================
# LIMPIAR NULOS PARA SALESFORCE
# ==========================================================
Calificaciones = Calificaciones.replace({
    float('nan'): None,
    pd.NA: None,
    np.nan: None
})

# ==========================================================
# 6 Convertir a registros
# ==========================================================
preparar(sf, 'Calificaciones_areas_e__c', Calificaciones)
