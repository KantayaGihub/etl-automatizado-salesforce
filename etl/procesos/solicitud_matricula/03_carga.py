import pandas as pd
import numpy as np
from salesforce_bulk import SalesforceBulk
import json
from io import BytesIO
import os
from simple_salesforce import Salesforce
from salesforce_bulk.util import IteratorBytesIO



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
query = "SELECT Id FROM Formulario_SolicitudMatricula_e__c"
records = sf.query_all(query)['records']

if not records:
    print("No hay registros para eliminar.")
else:
    ids = [{"Id": r["Id"]} for r in records]

    # 2️⃣ Crear job de eliminación
    job = bulk.create_delete_job("Formulario_SolicitudMatricula_e__c", contentType='JSON')

    # 3️⃣ Enviar en lotes
    batch_size = 10000
    for i in range(0, len(ids), batch_size):
        batch_records = ids[i:i+batch_size]
        json_bytes = json.dumps(batch_records).encode('utf-8')
        bulk.post_batch(job, IteratorBytesIO(iter([json_bytes])))

    # 4️⃣ Cerrar job
    bulk.close_job(job)
    print(f"Se enviaron {len(ids)} registros para eliminación.")


# ---------------------------------------------------------
# 1. Cargar archivo limpio (salida del ETL)
# ---------------------------------------------------------

input_path = "salida/Sol_Mtr_Deduplicado.csv"

print(f"📥 Cargando archivo: {input_path}")

df = pd.read_csv(
    input_path,
    dtype={
        "Número de Documento": str,
        "Número de Documento.1": str
    }
)

# ---------------------------------------------------------
# 2. Mapeo de columnas → Salesforce
# ---------------------------------------------------------

mapeo = {
  # Campos básicos
    "Marca temporal": "Marca_temporal__c",
    "Dirección de correo electrónico": "Direccin_de_correo_electrnico__c",
    "Nombre/s del niño/a": "Nombres_del_nioa__c",
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

    # Hermanos (coinciden con el patrón de Salesforce)
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

    # Campos de apoderado y padres
    "Nombre/s del apoderado del niño/a": "Nombres_del_apoderado_del_nioa__c",
    "Apellido Paterno.1": "Apellido_Paterno1__c",
    "Apellido Materno.1": "Apellido_Materno1__c",
    "Documento de Identidad.1": "Documento_de_Identidad1__c",
    "Número de Documento.1": "Nmero_de_Documento1__c",

    # Datos de contacto y ubicación
    "Número de celular": "Nmero_de_celular__c",
    "Departamento": "Departamento__c",
    "Provincia": "Provincia__c",
    "Distrito": "Distrito__c",
    "Tipo de Zona": "Tipo_de_Zona__c",
    "Dirección completa": "Direccin_completa__c",
    "Punto de referencia ": "Punto_de_referencia__c",
    "¿Vive con el niño/a?": "Vive_con_el_nioa__c",
    "Distritos": "Distritos__c",

    # Otros campos adicionales
    "¿Tiene otros hijos en inicial o primaria que no estén en el Programa Kantaya?": "Tiene_otros_hijos_en_inicial_o_primaria__c",
    "¿Cuántos?.1": "Cuntos1__c",
    "EDAD": "EDAD__c",
    "Parentesco": "Parentesco__c"
}

# Aplicar mapeo
df = df.rename(columns=mapeo)

for col in ["Fecha_de_nacimiento__c", "Marca_temporal__c"]:
    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
    df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)

df["EDAD__c"] = pd.to_numeric(df["EDAD__c"], errors = "coerce")

df = df.where(pd.notnull(df), None)


# Conexión Bulk API
bulk = SalesforceBulk(
    username='salesforce@kantayaperu.com.t4t',
    password='4uto.KP25',
    security_token='5BYv5tsEL1Iu8hL2bEXIFed5',
    sandbox=True
)

# 🔹 Limpiar valores NaN antes de convertir
df = df.replace({float('nan'): None, pd.NA: None, np.nan: None})

# Convertir DataFrame a lista de diccionarios
records = df.to_dict('records')

# Crear el job para insertar en el objeto personalizado
job = bulk.create_insert_job("Formulario_SolicitudMatricula_e__c", contentType='JSON')

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






