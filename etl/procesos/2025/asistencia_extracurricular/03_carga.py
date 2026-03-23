import pandas as pd
import numpy as np
from salesforce_bulk import SalesforceBulk
import json
from io import BytesIO
import os
from simple_salesforce import Salesforce
from salesforce_bulk.util import IteratorBytesIO
import json
import numpy as np
from io import BytesIO
from salesforce_bulk import SalesforceBulk
import json
import numpy as np
from io import BytesIO
from salesforce_bulk import SalesforceBulk



print("=== Eliminación de resultados previos Salesforce ===")

# Conexión
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

# 1️⃣ Consultar todos los IDs del objeto personalizado
query = "SELECT Id FROM Asistencias_Extracurriculares_e__c"
records = sf.query_all(query)['records']

if not records:
    print("No hay registros para eliminar.")
else:
    ids = [{"Id": r["Id"]} for r in records]

    # 2️⃣ Crear job de eliminación
    job = bulk.create_delete_job("Asistencias_Extracurriculares_e__c", contentType='JSON')

    # 3️⃣ Enviar en lotes
    batch_size = 10000
    for i in range(0, len(ids), batch_size):
        batch_records = ids[i:i+batch_size]
        json_bytes = json.dumps(batch_records).encode('utf-8')
        bulk.post_batch(job, IteratorBytesIO(iter([json_bytes])))

    # 4️⃣ Cerrar job
    bulk.close_job(job)
    print(f"Se enviaron {len(ids)} registros para eliminación.")


print("=== Iniciando carga a Salesforce (Extracurriculares) ===")

# ================================================================
# 1️⃣ Cargar consolidado generado por el ETL
# ================================================================
print("🔎 Cargando archivo consolidado de asistencias extracurriculares...")

ruta_consolidado = "salida/asistencias_extracurriculares/asistencias_extra_consolidado_kantaya.csv"

Asistencias = pd.read_csv(
    ruta_consolidado,
    sep=",",
    dtype={"DNI": str}
)

print(f"✔ Archivo leído correctamente: {len(Asistencias)} filas")

# ================================================================
# 2️⃣ Convertir columnas de fecha a formato YYYY-MM-DD
# ================================================================

for col in ["F_INCORPORACION", "F_SALIDA", "FECHA"]:
    Asistencias[col] = pd.to_datetime(Asistencias[col], errors='coerce', dayfirst=True)
    Asistencias[col] = Asistencias[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)

Asistencias["A_ESPERADAS"] = pd.to_numeric(Asistencias["A_ESPERADAS"], errors = "coerce")
Asistencias["A_REALES"] = pd.to_numeric(Asistencias["A_REALES"], errors = "coerce")
Asistencias["PORC_PART"] = pd.to_numeric(Asistencias["PORC_PART"], errors = "coerce")

Asistencias["CENTRO"] = Asistencias["CENTRO"].str.replace(
    " REGISTRO DE ASISTENCIA 2025.xlsx", "", regex=False
)

Asistencias["CENTRO"] = Asistencias["CENTRO"].str.replace(
    "_", " ", regex=False
)

Asistencias = Asistencias.where(pd.notnull(Asistencias), None)

Asistencias.columns = [col + "__c" for col in Asistencias.columns]


# Conexión Bulk API
bulk = SalesforceBulk(
    username='salesforce@kantayaperu.com.t4t',
    password='4uto.KP26',
    security_token='Urfjx1FzGoVLNSK0MlEKY16C',
    sandbox=True
)

# 🔹 Limpiar valores NaN antes de convertir
Asistencias = Asistencias.replace({float('nan'): None, pd.NA: None, np.nan: None})

# Convertir DataFrame a lista de diccionarios
records = Asistencias.to_dict('records')

# Crear el job para insertar en el objeto personalizado
job = bulk.create_insert_job("Asistencias_Extracurriculares_e__c", contentType='JSON')

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




























