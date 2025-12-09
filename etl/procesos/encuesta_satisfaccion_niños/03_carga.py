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



print("=== Eliminación de resultados previos Salesforce ===")

# Conexión
sf = Salesforce(
    username='salesforce@kantayaperu.com.t4t',
    password='4uto.KP25',
    security_token='5BYv5tsEL1Iu8hL2bEXIFed5',
    domain='test'
)

bulk = SalesforceBulk(
    username='salesforce@kantayaperu.com.t4t',
    password='4uto.KP25',
    security_token='5BYv5tsEL1Iu8hL2bEXIFed5',
    sandbox=True
)

# 1️⃣ Consultar todos los IDs del objeto personalizado
query = "SELECT Id FROM BD_Satisfaccion_Nino_e__c"
records = sf.query_all(query)['records']

if not records:
    print("No hay registros para eliminar.")
else:
    ids = [{"Id": r["Id"]} for r in records]

    # 2️⃣ Crear job de eliminación
    job = bulk.create_delete_job("BD_Satisfaccion_Nino_e__c", contentType='JSON')

    # 3️⃣ Enviar en lotes
    batch_size = 10000
    for i in range(0, len(ids), batch_size):
        batch_records = ids[i:i+batch_size]
        json_bytes = json.dumps(batch_records).encode('utf-8')
        bulk.post_batch(job, IteratorBytesIO(iter([json_bytes])))

    # 4️⃣ Cerrar job
    bulk.close_job(job)
    print(f"Se enviaron {len(ids)} registros para eliminación.")



print("=== Cargando archivo fuente ===")

OBJECT_NAME = "Satisfaccion_ninos_e__c"   # Nombre del objeto en Salesforce
INPUT_PATH = "entrada/Encuesta_Satisfaccion_Ninos.xlsx"

BD1 = pd.read_excel(
    INPUT_PATH,
    dtype={"DNI": str, "AÑO": str}
)

# ======================================================
# 🔄 MAPEO SIMPLE
# ======================================================
BD1 = BD1.rename(columns={
    "DNI": "DNI__c",
    "AÑO": "AO__c",
    "SEXO": "SEXO__c",
    "GRADO": "GRADO__c",
    "CONDICION ACTUAL": "CONDICION_ACTUAL__c",
    "CENTRO": "CENTRO__c",
    "Indicador_1": "Indicador_1__c",
    "Indicador_2": "Indicador_2__c",
    "Indicador_3": "Indicador_3__c",
    "Indicador_4": "Indicador_4__c"
})

# Conexión Bulk API
bulk = SalesforceBulk(
    username='salesforce@kantayaperu.com.t4t',
    password='4uto.KP25',
    security_token='5BYv5tsEL1Iu8hL2bEXIFed5',
    sandbox=True
)

# 🔹 Limpiar valores NaN antes de convertir
BD1 = BD1.replace({float('nan'): None, pd.NA: None, np.nan: None})

# Convertir DataFrame a lista de diccionarios
records = BD1.to_dict('records')

# Crear el job para insertar en el objeto personalizado
job = bulk.create_insert_job("BD_Satisfaccion_Nino_e__c", contentType='JSON')

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
















