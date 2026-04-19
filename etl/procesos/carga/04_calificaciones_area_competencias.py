import os
import pandas as pd
import numpy as np
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO
import json
from io import BytesIO
from pathlib import Path

print("=== ELIMINANDO REGISTROS PREVIOS ===")

# ================================================================
# 1️⃣ Variables de entorno
# ================================================================
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")

if not SF_USERNAME or not SF_PASSWORD or not SF_SECURITY_TOKEN:
    raise ValueError(
        "Faltan variables de entorno. Verifica SF_USERNAME, SF_PASSWORD y SF_SECURITY_TOKEN."
    )

# ================================================================
# 1️⃣ Conexión a Salesforce
# ================================================================
sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain="test"
)

bulk = SalesforceBulk(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    sandbox=True
)

# ==========================================================
# 2️⃣ Eliminar registros existentes
# ==========================================================
query = "SELECT Id FROM Calificaciones_areas_e__c"
records = sf.query_all(query)['records']

if not records:
    print("No hay registros para eliminar.")
else:
    ids = [{"Id": r["Id"]} for r in records]

    job = bulk.create_delete_job("Calificaciones_areas_e__c", contentType='JSON')

    batch_size = 10000
    for i in range(0, len(ids), batch_size):
        batch_records = ids[i:i+batch_size]
        json_bytes = json.dumps(batch_records).encode('utf-8')
        bulk.post_batch(job, IteratorBytesIO(iter([json_bytes])))

    bulk.close_job(job)
    print(f"Se enviaron {len(ids)} registros para eliminación.")

print("=== INICIANDO CARGA ===")

# ==========================================================
# 3️⃣ Leer consolidado
# ==========================================================
ruta_consolidado = Path(
    "data/consolidated/calificaciones_area_competencia/BD_Promedios_Areas_LB_LS_Consolidado.csv"
)

if not ruta_consolidado.exists():
    raise FileNotFoundError(f"No existe el consolidado: {ruta_consolidado}")

Calificaciones = pd.read_csv(ruta_consolidado)

print(f"✔ Archivo leído correctamente: {len(Calificaciones)} filas")

# ==========================================================
# 4️⃣ IMPORTANTE: NO agregar __c (ya vienen)
# ==========================================================
# 👉 Aquí NO hacemos:
# Calificaciones.columns = [col + "__c"]

# ==========================================================
# 5️⃣ Limpiar NaN (obligatorio para Salesforce)
# ==========================================================
Calificaciones = Calificaciones.replace({
    float('nan'): None,
    pd.NA: None,
    np.nan: None
})

# ==========================================================
# 6️⃣ Convertir a registros
# ==========================================================
records = Calificaciones.to_dict('records')

# ==========================================================
# 7️⃣ Insertar en Salesforce
# ==========================================================
job = bulk.create_insert_job("Calificaciones_areas_e__c", contentType='JSON')

batch_size = 1000

for i in range(0, len(records), batch_size):
    batch_records = records[i:i+batch_size]

    json_data = json.dumps(
        batch_records,
        ensure_ascii=False,
        allow_nan=False
    )

    batch_io = BytesIO(json_data.encode('utf-8'))
    bulk.post_batch(job, batch_io)

    print(f"Lote {i//batch_size + 1} enviado ({len(batch_records)} registros)")

bulk.close_job(job)

print("✅ Proceso enviado a Salesforce")
