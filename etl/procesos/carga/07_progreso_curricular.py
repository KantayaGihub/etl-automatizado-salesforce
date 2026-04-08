import json
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO

print("=== Eliminación de resultados previos Salesforce ===")

# ==========================================================
# 1️⃣ Conexión a Salesforce
# ==========================================================
sf = Salesforce(
    username='salesforce@kantayaperu.com.t4t',
    password='4uto.KP26',
    security_token='Urfjx1FzGoVLNSK0MlEKY16C',
    domain='test' 
)

bulk = SalesforceBulk(
    username='salesforce@kantayaperu.com.t4t',
    password='4uto.KP26',
    security_token='Urfjx1FzGoVLNSK0MlEKY16C',
    sandbox=True
)

OBJECT_NAME = "Progreso_curricular_e__c"

# ==========================================================
# 2️⃣ Eliminar registros previos del objeto
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
    print(f"Se enviaron {len(ids)} registros para eliminación.")

print("=== Cargando archivo consolidado ===")

# ==========================================================
# 3️⃣ Cargar consolidado
# ==========================================================
INPUT_PATH = Path("data/consolidated/progreso_curricular/BD_Curricula_Consolidada.csv")

if not INPUT_PATH.exists():
    raise FileNotFoundError(f"No existe el consolidado: {INPUT_PATH}")

Progreso_Curricular = pd.read_csv(INPUT_PATH)

print(f"✔ Archivo leído correctamente: {len(Progreso_Curricular)} filas")

# ==========================================================
# 4️⃣ Mantener trazabilidad si existe
# ==========================================================
if "ANIO_FUENTE" in Progreso_Curricular.columns:
    Progreso_Curricular = Progreso_Curricular.rename(
        columns={"ANIO_FUENTE": "ANIO_FUENTE__c"}
    )

# ==========================================================
# 5️⃣ Agregar sufijo __c a columnas que aún no lo tienen
# ==========================================================
Progreso_Curricular.columns = [
    col if str(col).endswith("__c") else f"{col}__c"
    for col in Progreso_Curricular.columns
]

# ==========================================================
# 6️⃣ Reemplazar nulos
# ==========================================================
Progreso_Curricular = Progreso_Curricular.where(pd.notnull(Progreso_Curricular), None)
Progreso_Curricular = Progreso_Curricular.replace({
    float('nan'): None,
    pd.NA: None,
    np.nan: None
})

# ==========================================================
# 7️⃣ Debug opcional
# ==========================================================
output_debug = Path("data/consolidated/progreso_curricular/DEBUG_para_salesforce.csv")
output_debug.parent.mkdir(parents=True, exist_ok=True)
Progreso_Curricular.to_csv(output_debug, index=False, encoding="utf-8-sig")
print(f"📁 Archivo final guardado para revisión: {output_debug}")

# ==========================================================
# 8️⃣ Convertir DataFrame a lista de diccionarios
# ==========================================================
records = Progreso_Curricular.to_dict('records')

# ==========================================================
# 9️⃣ Crear job para insertar
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
    print(f"Lote {i // batch_size + 1} enviado ({len(batch_records)} registros)")

bulk.close_job(job)

print("✅ Proceso enviado a Salesforce. Revisa resultados con bulk.get_all_batches(job)")
