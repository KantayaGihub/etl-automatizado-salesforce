import os
import json
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO

print("=== ELIMINACIÓN DE REGISTROS PREVIOS ===")

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

OBJECT_NAME = "BD_Habilidades_e__c"

# ==========================================================
# 2️⃣ ELIMINAR REGISTROS EXISTENTES
# ==========================================================
query = f"SELECT Id FROM {OBJECT_NAME}"
records = sf.query_all(query)['records']

if not records:
    print("No hay registros para eliminar.")
else:
    ids = [{"Id": r["Id"]} for r in records]

    job = bulk.create_delete_job(OBJECT_NAME, contentType='JSON')

    batch_size = 10000
    for i in range(0, len(ids), batch_size):
        batch_records = ids[i:i+batch_size]
        json_bytes = json.dumps(batch_records).encode('utf-8')
        bulk.post_batch(job, IteratorBytesIO(iter([json_bytes])))

    bulk.close_job(job)
    print(f"🗑️ Eliminados {len(ids)} registros")


print("=== CARGANDO CONSOLIDADO ===")

# ==========================================================
# 3️⃣ LEER CONSOLIDADO
# ==========================================================
INPUT_PATH = Path("data/consolidated/habilidades/BD_impacto_habilidades.csv")

if not INPUT_PATH.exists():
    raise FileNotFoundError(f"No existe el archivo: {INPUT_PATH}")

df = pd.read_csv(INPUT_PATH, dtype={"DNI__c": str})

print(f"✔ Registros leídos: {len(df)}")


# ==========================================================
# 4️⃣ RENOMBRE PARA SALESFORCE
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
# 5️⃣ LIMPIAR NULOS
# ==========================================================
df = df.where(pd.notnull(df), None)
df = df.replace({
    float('nan'): None,
    pd.NA: None,
    np.nan: None
})


# ==========================================================
# 6️⃣ DEBUG (CLAVE 🔥)
# ==========================================================
debug_path = Path("data/consolidated/habilidades/DEBUG_para_salesforce.csv")
debug_path.parent.mkdir(parents=True, exist_ok=True)

df.to_csv(debug_path, index=False, encoding="utf-8-sig")
print(f"📁 DEBUG guardado en: {debug_path}")


# ==========================================================
# 7️⃣ CONVERTIR A RECORDS
# ==========================================================
records = df.to_dict('records')


# ==========================================================
# 8️⃣ INSERTAR EN SALESFORCE
# ==========================================================
job = bulk.create_insert_job(OBJECT_NAME, contentType='JSON')

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

    print(f"🚀 Lote {i//batch_size + 1} enviado ({len(batch_records)} registros)")

bulk.close_job(job)

print("✅ Carga completada correctamente")
