import pandas as pd
import numpy as np
from salesforce_bulk import SalesforceBulk
import json
from io import BytesIO
from simple_salesforce import Salesforce
from salesforce_bulk.util import IteratorBytesIO

print("=== Eliminación de resultados previos Salesforce ===")

# ================================================================
# 1️⃣ Conexión a Salesforce
# ================================================================
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

# ================================================================
# 2️⃣ Eliminar registros previos del objeto personalizado
# ================================================================
query = "SELECT Id FROM Asistencias_e7__c"
records = sf.query_all(query)['records']

if not records:
    print("No hay registros para eliminar.")
else:
    ids = [{"Id": r["Id"]} for r in records]

    job = bulk.create_delete_job("Asistencias_e7__c", contentType='JSON')

    batch_size = 10000
    for i in range(0, len(ids), batch_size):
        batch_records = ids[i:i+batch_size]
        json_bytes = json.dumps(batch_records).encode('utf-8')
        bulk.post_batch(job, IteratorBytesIO(iter([json_bytes])))

    bulk.close_job(job)
    print(f"Se enviaron {len(ids)} registros para eliminación.")


print("=== Iniciando carga a Salesforce ===")

# ================================================================
# 3️⃣ Cargar consolidado generado por la consolidación
# ================================================================
print("🔎 Cargando archivo consolidado de asistencias...")

ruta_consolidado = "data/consolidated/asistencia_regular/asistencias_consolidado_kantaya.csv"

Asistencias = pd.read_csv(
    ruta_consolidado,
    sep=",",
    dtype={"DNI": str}
)

print(f"✔ Archivo leído correctamente: {len(Asistencias)} filas")


# ================================================================
# 4️⃣ Convertir columnas de fecha a formato YYYY-MM-DD
# ================================================================
for col in ["F_INCORPORACION", "F_SALIDA", "FECHA"]:
    if col in Asistencias.columns:
        Asistencias[col] = pd.to_datetime(
            Asistencias[col], errors='coerce', dayfirst=True
        )
        Asistencias[col] = Asistencias[col].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None
        )
    else:
        print(f"⚠️ Advertencia: No se encontró columna {col} en el consolidado")


# ================================================================
# 5️⃣ Convertir métricas numéricas
# ================================================================
for col in ["A_ESPERADAS", "A_REALES", "PORC_PART"]:
    if col in Asistencias.columns:
        Asistencias[col] = pd.to_numeric(
            Asistencias[col], errors="coerce"
        )
    else:
        print(f"⚠️ Advertencia: No se encontró columna {col} en el consolidado")


# ================================================================
# 6️⃣ Reemplazar NaN por None
# ================================================================
Asistencias = Asistencias.where(pd.notnull(Asistencias), None)


# ================================================================
# 7️⃣ Renombrar columnas con sufijo __c
# ================================================================
Asistencias.columns = [col + "__c" for col in Asistencias.columns]

print("✔ Archivo preparado correctamente para Salesforce Bulk API")


# ================================================================
# 8️⃣ Reconexión Bulk API para inserción
# ================================================================
bulk = SalesforceBulk(
    username='salesforce@kantayaperu.com.t4t',
    password='4uto.KP26',
    security_token='Urfjx1FzGoVLNSK0MlEKY16C',
    sandbox=True
)

# Limpiar NaN antes de convertir
Asistencias = Asistencias.replace({float('nan'): None, pd.NA: None, np.nan: None})

# Convertir DataFrame a lista de diccionarios
records = Asistencias.to_dict('records')

# Crear job para insertar
job = bulk.create_insert_job("Asistencias_e7__c", contentType='JSON')

# Subir en batches
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

# Cerrar job
bulk.close_job(job)

print("✅ Proceso enviado a Salesforce. Revisa resultados con bulk.get_all_batches(job)")
