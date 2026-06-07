import os
import time
import json
from io import BytesIO

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO


print("================================================")
print("=== ETL CARGA SALESFORCE - ASISTENCIA REGULAR ===")
print("================================================")


OBJECT_NAME = "Asistencias_e7__c"
RUTA_CONSOLIDADO = "data/consolidated/asistencia_regular/asistencias_consolidado_kantaya.csv"
DELETE_BATCH_SIZE = 10000
INSERT_BATCH_SIZE = 1000


def wait_for_batch(bulk_client, job_id, batch_id):
    while not bulk_client.is_batch_done(batch_id, job_id):
        time.sleep(5)


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

bulk = SalesforceBulk(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    sandbox=False
)

print("Conexion Salesforce OK")


print("\n=== ELIMINANDO REGISTROS PREVIOS ===")

query = f"SELECT Id FROM {OBJECT_NAME}"
records_delete = sf.query_all(query)["records"]

if not records_delete:
    print("No existen registros previos")
else:
    ids = [{"Id": r["Id"]} for r in records_delete]
    print(f"Registros encontrados para eliminar: {len(ids):,}")

    delete_job = bulk.create_delete_job(OBJECT_NAME, contentType="JSON")
    delete_batches = []

    for i in range(0, len(ids), DELETE_BATCH_SIZE):
        batch_records = ids[i:i + DELETE_BATCH_SIZE]
        json_bytes = json.dumps(batch_records).encode("utf-8")
        batch_id = bulk.post_batch(
            delete_job,
            IteratorBytesIO(iter([json_bytes]))
        )
        delete_batches.append(batch_id)
        print(f"Batch delete enviado {i // DELETE_BATCH_SIZE + 1}")

    bulk.close_job(delete_job)

    print("Esperando finalizacion de eliminaciones...")
    for batch_id in delete_batches:
        wait_for_batch(bulk, delete_job, batch_id)

    print("Eliminacion completada")


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


print("\n=== ENVIANDO A SALESFORCE BULK API ===")

records = Asistencias.to_dict('records')
print(f"Registros a insertar: {len(records):,}")

insert_job = bulk.create_insert_job(OBJECT_NAME, contentType='JSON')

for i in range(0, len(records), INSERT_BATCH_SIZE):
    batch_records = records[i:i + INSERT_BATCH_SIZE]
    json_data = json.dumps(
        batch_records,
        ensure_ascii=False,
        allow_nan=False
    )
    batch_io = BytesIO(json_data.encode('utf-8'))
    bulk.post_batch(insert_job, batch_io)
    print(f"Lote {i // INSERT_BATCH_SIZE + 1} enviado ({len(batch_records)} registros)")

bulk.close_job(insert_job)

print("\nProceso enviado a Salesforce correctamente")
