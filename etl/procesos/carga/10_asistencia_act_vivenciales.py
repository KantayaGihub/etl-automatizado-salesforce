import json
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
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


OBJECT_NAME = "asistencia_actividades_vivenciales__c"

# ==========================================================
# 2️⃣ Eliminar registros previos
# ==========================================================
query = f"SELECT Id FROM {OBJECT_NAME}"
records = sf.query_all(query)["records"]

if not records:
    print("No hay registros para eliminar.")
else:
    ids = [{"Id": r["Id"]} for r in records]

    job = bulk.create_delete_job(OBJECT_NAME, contentType="JSON")

    batch_size = 10000
    for i in range(0, len(ids), batch_size):
        batch_records = ids[i:i + batch_size]
        json_bytes = json.dumps(batch_records).encode("utf-8")
        bulk.post_batch(job, IteratorBytesIO(iter([json_bytes])))

    bulk.close_job(job)
    print(f"Se enviaron {len(ids)} registros para eliminación.")

print("=== Iniciando carga a Salesforce ===")

# ==========================================================
# 3️⃣ Leer consolidado
# ==========================================================
input_path = Path(
    "data/consolidated/asistencia_actividades_vivenciales/BD_asistencia_actividades_vivenciales.csv"
)

if not input_path.exists():
    raise FileNotFoundError(f"No existe el consolidado: {input_path}")

df = pd.read_csv(
    input_path,
    dtype={"DNI": str}
)

print(f"✔ Archivo leído correctamente: {len(df)} filas")

# ==========================================================
# 4️⃣ Convertir columnas
# ==========================================================
if "FECHA" in df.columns:
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce", dayfirst=True)
    df["FECHA"] = df["FECHA"].apply(
        lambda x: x.strftime("%Y-%m-%d") if pd.notnull(x) else None
    )

for col in ["ASISTENCIAS PROGRAMADAS", "ASISTENCIAS REALES", "% PART."]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ==========================================================
# 5️⃣ Renombrar columnas para Salesforce
# ==========================================================
mapeo = {
    "FECHA": "FECHA__c",
    "VALOR": "VALOR__c",
    "DNI": "DNI__c",
    "GRADO": "GRADO__c",
    "ASISTENCIAS PROGRAMADAS": "ASISTENCIAS_PROGRAMADAS__c",
    "MES": "MES__c",
    "ANIO_FUENTE": "ANIO_FUENTE__c",
    "ASISTENCIAS REALES": "ASISTENCIAS_REALES__c",
    "ACTIVIDAD": "ACTIVIDAD__c",
    "ESTADO": "ESTADO__c",
    "CONDICIÓN DE PARTICIPACIÓN": "CONDICIN_DE_PARTICIPACIN__c",
    "SEXO": "SEXO__c",
    "NOMBRES Y APELLIDOS": "NOMBRES_Y_APELLIDOS__c",
    "CENTRO": "CENTRO__c",
    "% PART.": "PART__c",
}

df = df.rename(columns=mapeo)

print("✔ Columnas renombradas para Salesforce")

# ==========================================================
# 6️⃣ Limpiar nulos
# ==========================================================
df = df.where(pd.notnull(df), None)
df = df.replace({
    float("nan"): None,
    pd.NA: None,
    np.nan: None
})

# ==========================================================
# 7️⃣ Guardar debug
# ==========================================================
debug_path = Path(
    "data/consolidated/asistencia_actividades_vivenciales/DEBUG_para_salesforce.csv"
)
debug_path.parent.mkdir(parents=True, exist_ok=True)

df.to_csv(debug_path, index=False, encoding="utf-8-sig")
print(f"📁 [DEBUG] DataFrame final guardado en: {debug_path}")

# ==========================================================
# 8️⃣ Convertir a records
# ==========================================================
records = df.to_dict("records")

# ==========================================================
# 9️⃣ Insertar en Salesforce
# ==========================================================
job = bulk.create_insert_job(OBJECT_NAME, contentType="JSON")

batch_size = 1000

for i in range(0, len(records), batch_size):
    batch_records = records[i:i + batch_size]
    json_data = json.dumps(
        batch_records,
        ensure_ascii=False,
        allow_nan=False
    )
    batch_io = BytesIO(json_data.encode("utf-8"))
    bulk.post_batch(job, batch_io)
    print(f"Lote {i // batch_size + 1} enviado ({len(batch_records)} registros)")

bulk.close_job(job)

print("✅ Proceso enviado a Salesforce")
