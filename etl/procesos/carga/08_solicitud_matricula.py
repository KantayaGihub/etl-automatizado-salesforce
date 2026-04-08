import os
import json
from io import BytesIO
from pathlib import Path

import pandas as pd
import numpy as np
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

OBJECT_NAME = "Formulario_SolicitudMatricula_e__c"

# ==========================================================
# 2️⃣ Eliminar registros previos
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

print("=== Iniciando carga a Salesforce ===")

# ==========================================================
# 3️⃣ Cargar consolidado
# ==========================================================
input_path = Path("data/consolidated/solicitud_matricula/Sol_Mtr_Deduplicado.csv")

if not input_path.exists():
    raise FileNotFoundError(f"No existe el consolidado: {input_path}")

print(f"📥 Cargando archivo: {input_path}")

df = pd.read_csv(
    input_path,
    dtype={
        "Número de Documento": str,
        "Número de Documento.1": str
    }
)

print(f"✔ Archivo leído correctamente: {len(df)} filas")

# ==========================================================
# 4️⃣ Mantener ANIO_FUENTE si existe
# ==========================================================
if "ANIO_FUENTE" in df.columns:
    df = df.rename(columns={"ANIO_FUENTE": "ANIO_FUENTE__c"})

# ==========================================================
# 5️⃣ Mapeo de columnas → Salesforce
# ==========================================================
mapeo = {
    "Marca temporal": "Marca_temporal__c",
    "Dirección de correo electrónico": "Direccin_de_correo_electrnico__c",
    "Nombre/S Del Niño/A": "Nombres_del_nioa__c",
    "Apellido Paterno": "Apellido_Paterno__c",
    "Apellido Materno": "Apellido_Materno__c",
    "Documento de Identidad": "Documento_de_Identidad__c",
    "Número de Documento": "Nmero_de_Documento__c",
    "Sexo": "Sexo__c",
    "Fecha de nacimiento": "Fecha_de_nacimiento__c",
    "Centro al que postula": "Centro_al_que_postula__c",
    "Grado al que postula": "Grado_al_que_postula__c",
    "Nombre de institución educativa de procedencia": "Nombre_de_institucin_educativa_de_proce__c",
    "Grado actual en el colegio": "Grado_actual_en_el_colegio__c",
    "Sección actual en el colegio": "Seccin_actual_en_el_colegio__c",
    "País de nacimiento": "Pas_de_nacimiento__c",
    "Departamento de nacimiento": "Departamento_de_nacimiento__c",
    "¿Cuenta con algún tipo de discapacidad?": "Cuenta_con_algn_tipo_de_discapacidad__c",
    "¿Cuenta con el carnet de CONADIS?": "Cuenta_con_el_carnet_de_CONADIS__c",
    "¿Qué discapacidad tiene?": "Qu_discapacidad_tiene__c",
    "¿Tiene hermanos en el programa Kantaya?": "Tiene_hermanos_en_el_programa_Kantaya__c",
    "¿Cuántos?": "Cuntos__c",
    "Nombre, apellido paterno y materno": "Nombre_apellido_paterno_y_materno__c",

    "Hermano 1: Nombre, apellido paterno y materno": "Hermano_1_Nombre_apellido_paterno_y_ma__c",
    "Hermano 1: Nombre, apellido paterno y materno.1": "Hermano_1_Nombre_apellido_paterno_y_ma_1__c",
    "Hermano 1: Nombre, apellido paterno y materno.2": "Hermano_1_Nombre_apellido_paterno_y_ma_2__c",
    "Hermano 1: Nombre, apellido paterno y materno.3": "Hermano_1_Nombre_apellido_paterno_y_ma_3__c",
    "Hermano 1: Nombre, apellido paterno y materno.4": "Hermano_1_Nombre_apellido_paterno_y_ma_4__c",

    "Hermano 2: Nombre, apellido paterno y materno": "Hermano_2_Nombre_apellido_paterno_y_ma__c",
    "Hermano 2: Nombre, apellido paterno y materno.1": "Hermano_2_Nombre_apellido_paterno_y_ma_1__c",
    "Hermano 2: Nombre, apellido paterno y materno.2": "Hermano_2_Nombre_apellido_paterno_y_ma_2__c",
    "Hermano 2: Nombre, apellido paterno y materno.3": "Hermano_2_Nombre_apellido_paterno_y_ma_3__c",
    "Hermano 2: Nombre, apellido paterno y materno.4": "Hermano_2_Nombre_apellido_paterno_y_ma_4__c",

    "Hermano 3: Nombre, apellido paterno y materno": "Hermano_3_Nombre_apellido_paterno_y_ma__c",
    "Hermano 3: Nombre, apellido paterno y materno.1": "Hermano_3_Nombre_apellido_paterno_y_ma_1__c",
    "Hermano 3: Nombre, apellido paterno y materno.2": "Hermano_3_Nombre_apellido_paterno_y_ma_2__c",
    "Hermano 3: Nombre, apellido paterno y materno.3": "Hermano_3_Nombre_apellido_paterno_y_ma_3__c",

    "Hermano 4: Nombre, apellido paterno y materno": "Hermano_4_Nombre_apellido_paterno_y_ma__c",
    "Hermano 4: Nombre, apellido paterno y materno.1": "Hermano_4_Nombre_apellido_paterno_y_ma_1__c",
    "Hermano 4: Nombre, apellido paterno y materno.2": "Hermano_4_Nombre_apellido_paterno_y_ma_2__c",

    "Hermano 5: Nombre, apellido paterno y materno": "Hermano_5_Nombre_apellido_paterno_y_ma__c",
    "Hermano 5: Nombre, apellido paterno y materno.1": "Hermano_5_Nombre_apellido_paterno_y_ma_1__c",

    "Hermano 6: Nombre, apellido paterno y materno": "Hermano_6_Nombre_apellido_paterno_y_ma__c",
    "Hermano 7: Nombre, apellido paterno y materno": "Hermano_7_Nombre_apellido_paterno_y_ma__c",
    "Hermano 8: Nombre, apellido paterno y materno": "Hermano_8_Nombre_apellido_paterno_y_ma__c",
    "Hermano 9: Nombre, apellido paterno y materno": "Hermano_9_Nombre_apellido_paterno_y_ma__c",

    "Nombre/s del apoderado del niño/a": "Nombres_del_apoderado_del_nioa__c",
    "Apellido Paterno.1": "Apellido_Paterno1__c",
    "Apellido Materno.1": "Apellido_Materno1__c",
    "Documento de Identidad.1": "Documento_de_Identidad1__c",
    "Número de Documento.1": "Nmero_de_Documento1__c",

    "Número de celular": "Nmero_de_celular__c",
    "Departamento": "Departamento__c",
    "Provincia": "Provincia__c",
    "Distrito": "Distrito__c",
    "Tipo de Zona": "Tipo_de_Zona__c",
    "Dirección completa": "Direccin_completa__c",
    "Punto de referencia ": "Punto_de_referencia__c",
    "¿Vive con el niño/a?": "Vive_con_el_nioa__c",
    "Distritos": "Distritos__c",

    "¿Tiene otros hijos en inicial o primaria que no estén en el Programa Kantaya?": "Tiene_otros_hijos_en_inicial_o_primaria__c",
    "¿Cuántos?.1": "Cuntos1__c",
    "EDAD": "EDAD__c",
    "Parentesco": "Parentesco__c",
    "ANIO_FUENTE": "ANIO_FUENTE__c"
}

df = df.rename(columns=mapeo)

# ==========================================================
# 6️⃣ Convertir fechas
# ==========================================================
for col in ["Fecha_de_nacimiento__c", "Marca_temporal__c"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
        df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)

# ==========================================================
# 7️⃣ Convertir numéricos
# ==========================================================
if "EDAD__c" in df.columns:
    df["EDAD__c"] = pd.to_numeric(df["EDAD__c"], errors="coerce")

# ==========================================================
# 8️⃣ Limpiar nulos
# ==========================================================
df = df.where(pd.notnull(df), None)
df = df.replace({float('nan'): None, pd.NA: None, np.nan: None})

# ==========================================================
# 9️⃣ Guardar debug
# ==========================================================
debug_path = Path("data/consolidated/solicitud_matricula/DEBUG_para_salesforce.csv")
debug_path.parent.mkdir(parents=True, exist_ok=True)

df.to_csv(debug_path, index=False, encoding="utf-8-sig")
print(f"📁 [DEBUG] DataFrame final guardado en: {debug_path}")

# ==========================================================
# 🔟 Convertir a records
# ==========================================================
records = df.to_dict('records')

# ==========================================================
# 1️⃣1️⃣ Insertar en Salesforce
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
    print(f"Lote {i//batch_size + 1} enviado ({len(batch_records)} registros)")

bulk.close_job(job)

print("✅ Proceso enviado a Salesforce. Revisa resultados con bulk.get_all_batches(job)")
