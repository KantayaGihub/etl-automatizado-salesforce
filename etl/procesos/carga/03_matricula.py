import pandas as pd
import numpy as np
from salesforce_bulk import SalesforceBulk
import json
from io import BytesIO
from simple_salesforce import Salesforce
from salesforce_bulk.util import IteratorBytesIO

print("=== Eliminación de resultados previos Salesforce ===")

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
# 2️⃣ Eliminar registros previos del objeto personalizado
# ==========================================================
query = "SELECT Id FROM Matricula_e__c"
records = sf.query_all(query)['records']

if not records:
    print("No hay registros para eliminar.")
else:
    ids = [{"Id": r["Id"]} for r in records]

    job = bulk.create_delete_job("Matricula_e__c", contentType='JSON')

    batch_size = 10000
    for i in range(0, len(ids), batch_size):
        batch_records = ids[i:i+batch_size]
        json_bytes = json.dumps(batch_records).encode('utf-8')
        bulk.post_batch(job, IteratorBytesIO(iter([json_bytes])))

    bulk.close_job(job)
    print(f"Se enviaron {len(ids)} registros para eliminación.")

print("=== Iniciando carga a Salesforce (Matrícula) ===")

# ==========================================================
# 3️⃣ Cargar consolidado generado por la consolidación
# ==========================================================
input_path = "data/consolidated/matricula/consolidado_matricula_afterschool.csv"

print(f"📥 Cargando archivo: {input_path}")

Matricula = pd.read_csv(
    input_path,
    sep=",",
    dtype={"DNI": str, "DNI DEL NIÑO": str, "DNI DEL NINO": str}
)

print(f"✔ Archivo leído correctamente: {len(Matricula)} filas")

# ==========================================================
# 4️⃣ Eliminación temporal de columnas
# ==========================================================
cols_drop = [
    'N.1', 'DNI.1', 'APELLIDOS Y NOMBRES.1', 'GRADO.1', 'SEXO.1',
    'CENTRO.1', 'PERIODO DE INGRESO.1', 'NUMERO TELEFONICO.1',
    'FECHA DE REGISTRO.1', 'RESPONSABLE DE REGISTRO.1',
    'CONDICION ACTUAL.1', 'DOCUMENTO EN SHAREPOINT.1',
    'FOTO DEL NINO (A).1', 'COPIA DNI/ CARNET DE EXTRANJERIA DEL MENOR .1',
    'COPIA DNI/ MADRE O APODERADO.1', 'RECIBO DE SERVICIOS (LUZ O AGUA).1',
    'BOLETA DE NOTAS.1', 'CARTA COMPROMISO.1', 'ACUERDO DE IMAGEN.1',
    'FICHA SOCIAL .1', 'SOLICITUD DE MATRICULA .1',
    'RENOVACION DE MATRICULA.1', 'N DE DOC. PRESENTADOS.1',
    'ESTADO FINAL MATRICULA.1'
]

Matricula = Matricula.drop(columns=cols_drop, errors='ignore')

# ==========================================================
# 5️⃣ Si existe columna de trazabilidad, quitarla antes de SF
# ==========================================================
if "ANIO_FUENTE" in Matricula.columns:
    Matricula = Matricula.drop(columns=["ANIO_FUENTE"])

# ==========================================================
# 6️⃣ Conversión de campos numéricos y fechas
# ==========================================================
if "N DE DOC. PRESENTADOS" in Matricula.columns:
    Matricula["N DE DOC. PRESENTADOS"] = pd.to_numeric(
        Matricula["N DE DOC. PRESENTADOS"], errors="coerce"
    )

for col in ["FECHA DE REGISTRO"]:
    if col in Matricula.columns:
        Matricula[col] = pd.to_datetime(Matricula[col], errors='coerce', dayfirst=True)
        Matricula[col] = Matricula[col].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None
        )

# ==========================================================
# 7️⃣ Reemplazar nulos
# ==========================================================
Matricula = Matricula.where(pd.notnull(Matricula), None)

# ==========================================================
# 8️⃣ Agregar sufijo __c
# ==========================================================
Matricula.columns = [col + "__c" for col in Matricula.columns]

# ==========================================================
# 9️⃣ Renombrar columnas para que coincidan con Salesforce
# ==========================================================
Matricula = Matricula.rename(columns={
    "APELLIDOS Y NOMBRES__c": "APELLIDOS_Y_NOMBRES__c",
    "BOLETA DE NOTAS__c": "BOLETA_DE_NOTAS__c",
    "FOTO DEL NINO (A)__c": "FOTO_DEL_NINO_A__c",
    "ESTADO FINAL MATRICULA__c": "ESTADO_FINAL_MATRICULA__c",
    "COPIA DNI/ CARNET DE EXTRANJERIA DEL MENOR__c": "COPIA_DNI_CARNET_DE_EXTRANJERIA_DEL_MEN__c",
    "COPIA DNI/ MADRE O APODERADO__c": "COPIA_DNI_MADRE_O_APODERADO__c",
    "RECIBO DE SERVICIOS (LUZ O AGUA)__c": "RECIBO_DE_SERVICIOS_LUZ_O_AGUA__c",
    "FICHA SOCIAL__c": "FICHA_SOCIAL__c",
    "SOLICITUD DE MATRICULA__c": "SOLICITUD_DE_MATRICULA__c",
    "ACUERDO DE IMAGEN__c": "ACUERDO_DE_IMAGEN__c",
    "RESPONSABLE DE REGISTRO__c": "RESPONSABLE_DE_REGISTRO__c",
    "PERIODO DE INGRESO__c": "PERIODO_DE_INGRESO__c",
    "CARTA COMPROMISO__c": "CARTA_COMPROMISO__c",
    "CONDICION ACTUAL__c": "CONDICION_ACTUAL__c",
    "DOCUMENTO EN SHAREPOINT__c": "DOCUMENTO_EN_SHAREPOINT__c",
    "RENOVACION DE MATRICULA__c": "RENOVACION_DE_MATRICULA__c",
    "N DE DOC. PRESENTADOS__c": "N_DE_DOC_PRESENTADOS__c",
    "FECHA DE REGISTRO__c": "FECHA_DE_REGISTRO__c",
    "NUMERO TELEFONICO__c": "NUMERO_TELEFONICO__c",
    "ANO DE INGRESO__c": "ANO_DE_INGRESO__c"
})

print("✔ Archivo preparado correctamente para Salesforce Bulk API")

# ==========================================================
# 🔟 Reconexión Bulk API
# ==========================================================
bulk = SalesforceBulk(
    username='salesforce@kantayaperu.com.t4t',
    password='4uto.KP26',
    security_token='Urfjx1FzGoVLNSK0MlEKY16C',
    sandbox=True
)

Matricula = Matricula.replace({float('nan'): None, pd.NA: None, np.nan: None})

# Convertir DataFrame a lista de diccionarios
records = Matricula.to_dict('records')

# Crear el job para insertar en el objeto personalizado
job = bulk.create_insert_job("Matricula_e__c", contentType='JSON')

# Subir registros en batches de 1,000
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

# ==========================================================
# 1️⃣1️⃣ Guardar archivo final para revisión
# ==========================================================
output_final = "data/consolidated/matricula/matricula_final_para_salesforce.csv"

Matricula.to_csv(
    output_final,
    index=False,
    encoding="utf-8-sig"
)

print(f"📁 Archivo final para Salesforce guardado en: {output_final}")
