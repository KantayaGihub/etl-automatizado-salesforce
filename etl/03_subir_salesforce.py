import pandas as pd
import numpy as np
from salesforce_bulk import SalesforceBulk
import json
from io import BytesIO
import os
from utils2.mapeo import mapeo

print("=== Eliminación de resultados previos Salesforce ===")


bulk = SalesforceBulk(
    username='salesforce@kantayaperu.com.t4t',
    password='4uto.KP25',
    security_token='5BYv5tsEL1Iu8hL2bEXIFed5',
    sandbox=True
)

# 1️⃣ Consultar todos los IDs del objeto personalizado
query = "SELECT Id FROM Ficha_social_e__c"
records = sf.query_all(query)['records']

if not records:
    print("No hay registros para eliminar.")
else:
    ids = [{"Id": r["Id"]} for r in records]

    # 2️⃣ Crear job de eliminación
    job = bulk.create_delete_job("Ficha_social_e__c", contentType='JSON')

    # 3️⃣ Enviar en lotes
    batch_size = 10000
    for i in range(0, len(ids), batch_size):
        batch_records = ids[i:i+batch_size]
        json_bytes = json.dumps(batch_records).encode('utf-8')
        bulk.post_batch(job, IteratorBytesIO(iter([json_bytes])))

    # 4️⃣ Cerrar job
    bulk.close_job(job)
    print(f"Se enviaron {len(ids)} registros para eliminación.")



print("=== Iniciando carga a Salesforce ===")

print("🔎 Cargando archivo limpio...")
df = pd.read_excel(
    "salida/Ficha_Social_v2.xlsx",
    dtype={
        "Número de Documento": str,
        "Número de Documento.1": str,
        "Número de documento del niño": str
    }
)

print("🔎 Cargando mapeo de utils2/mapeo.py...")
print("Mapeo contiene:", len(mapeo), "columnas")

# Aplicar mapeo
df = df.rename(columns=mapeo)

# Convertir todo a string excepto fecha
for col in df.columns:
    df[col] = df[col].astype(str)

# Limpiar NaN
df = df.replace({np.nan: None, pd.NA: None})

records = df.to_dict("records")

bulk = SalesforceBulk(
    username='salesforce@kantayaperu.com.t4t',
    password='4uto.KP25',
    security_token='5BYv5tsEL1Iu8hL2bEXIFed5',
    sandbox=True
)

job = bulk.create_insert_job("Ficha_social_e__c", contentType='JSON')

# Subir registros en batches de 1,000
batch_size = 1000

for i in range(0, len(records), batch_size):
    batch_records = records[i:i+batch_size]
    json_data = json.dumps(
        batch_records,
        ensure_ascii=False,
        allow_nan=False  # ahora no habrá NaN
    )
    batch_io = BytesIO(json_data.encode('utf-8'))
    bulk.post_batch(job, batch_io)
    print(f"Lote {i//batch_size + 1} enviado ({len(batch_records)} registros)")

# Cerrar job
bulk.close_job(job)

print("✅ Proceso enviado a Salesforce. Revisa resultados con bulk.get_all_batches(job)")


print("✅ ETL completado y enviado a Salesforce.")
