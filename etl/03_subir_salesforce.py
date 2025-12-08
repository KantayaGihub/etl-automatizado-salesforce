import pandas as pd
import numpy as np
from salesforce_bulk import SalesforceBulk
import json
from io import BytesIO
import os
from utils2.mapeo import mapeo



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
    if col != "Marca_temporal__c":
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
