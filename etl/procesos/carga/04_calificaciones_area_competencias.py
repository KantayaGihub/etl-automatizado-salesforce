import os
import pandas as pd
import numpy as np
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO
import json
from io import BytesIO
from pathlib import Path
import time


def wait_for_batch(bulk_client, job_id, batch_id):
    while not bulk_client.is_batch_done(batch_id, job_id):
        time.sleep(5)


print("=== ELIMINANDO REGISTROS PREVIOS ===")

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

bulk = SalesforceBulk(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    sandbox=False
)

# ==========================================================
# 2 Eliminar registros existentes
# ==========================================================
query = "SELECT Id FROM Calificaciones_areas_e__c"
records = sf.query_all(query)['records']

if not records:
    print("No hay registros para eliminar.")
else:
    ids = [{"Id": r["Id"]} for r in records]

    job = bulk.create_delete_job("Calificaciones_areas_e__c", contentType='JSON')

    delete_batches = []
    batch_size = 10000
    for i in range(0, len(ids), batch_size):
        batch_records = ids[i:i+batch_size]
        json_bytes = json.dumps(batch_records).encode('utf-8')
        batch_id = bulk.post_batch(job, IteratorBytesIO(iter([json_bytes])))
        delete_batches.append(batch_id)

    bulk.close_job(job)

    print("Esperando finalizacion de eliminaciones...")
    for batch_id in delete_batches:
        wait_for_batch(bulk, job, batch_id)

    print(f"Eliminacion completada: {len(ids)} registros")

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
records = Calificaciones.to_dict('records')

# ==========================================================
# 7 Insertar en Salesforce
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

print("Proceso enviado a Salesforce correctamente")
