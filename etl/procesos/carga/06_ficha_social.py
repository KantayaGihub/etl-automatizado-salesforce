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

SF_OBJECT = "Ficha_social_e__c"

# ==========================================================
# 2️⃣ Eliminar registros previos del objeto
# ==========================================================
query = f"SELECT Id FROM {SF_OBJECT}"
records = sf.query_all(query)['records']

if not records:
    print("No hay registros para eliminar.")
else:
    ids = [{"Id": r["Id"]} for r in records]

    job = bulk.create_delete_job(SF_OBJECT, contentType='JSON')

    batch_size = 10000
    for i in range(0, len(ids), batch_size):
        batch_records = ids[i:i+batch_size]
        json_bytes = json.dumps(batch_records).encode('utf-8')
        bulk.post_batch(job, IteratorBytesIO(iter([json_bytes])))

    bulk.close_job(job)
    print(f"Se enviaron {len(ids)} registros para eliminación.")

print("=== Iniciando carga a Salesforce (Ficha Social) ===")

# ==========================================================
# 3️⃣ Cargar consolidado
# ==========================================================
ruta_consolidado = Path("data/consolidated/ficha_social/Ficha_Social_v2.xlsx")

if not ruta_consolidado.exists():
    raise FileNotFoundError(f"No existe el consolidado: {ruta_consolidado}")

df = pd.read_excel(
    ruta_consolidado,
    sheet_name="Ficha_Social_v2",
    dtype={
        "Número de Documento": str,
        "Número de Documento.1": str,
        "Número de documento del niño": str
    }
)

print(f"✔ Archivo leído correctamente: {len(df)} filas")

# ==========================================================
# 4️⃣ Mapeo de columnas hacia Salesforce
# ==========================================================
mapeo = {
    "Marca temporal": "Marca_temporal__c",
    "Dirección de correo electrónico": "Direccin_de_correo_electrnico__c",
    "Nombre/s del niño/a": "Nombres_del_nioa__c",
    "Apellido Paterno": "Apellido_Paterno__c",
    "Apellido Materno": "Apellido_Materno__c",
    "¿Es alérgico a algún medicamento?": "Es_alrgico_a_algn_medicamento__c",
    "ID": "ID__c",
    "¿A qué medicamento es alérgico?": "A_qu_medicamento_es_alrgico__c",
    "Peso ": "Peso__c",
    "Talla ": "Talla__c",
    "¿Cuanto tiene de hemoglobina el niño/a? ": "Cuanto_tiene_de_hemoglobina_el_nioa__c",
    "Número de estudiantes en el salón de su colegio:": "Nmero_de_estudiantes_en_el_saln_de_su__c",
    "Religión que profesa la familia:": "Religin_que_profesa_la_familia__c",
    "¿Alguna vez ha repetido un grado?": "Alguna_vez_ha_repetido_un_grado__c",
    "¿Qué grado repitió?": "Qu_grado_repiti__c",

    "Nombre/S Del Apoderado 1 Del Niño/A": "NombreS_Del_Apoderado_1_Del_NioA__c",
    "Apellido Paterno.1": "Apellido_Paterno1__c",
    "Apellido Materno.1": "Apellido_materno_1__c",
    "Parentesco": "Parentesco__c",
    "Fecha de nacimiento": "Fecha_de_nacimiento__c",
    "Documento de identidad": "Documento_de_identidad__c",
    "Número de Documento": "Nmero_de_Documento__c",

    "País de nacimiento": "Pas_de_nacimiento__c",
    "Departamento de NACIMIENTO": "Departamento_de_NACIMIENTO__c",
    "Departamento": "Departamento__c",
    "Provincia": "Provincia__c",
    "Distrito": "Distrito__c",
    "Tipo de Zona": "Tipo_de_Zona__c",
    "Dirección completa": "Direccin_completa__c",
    "Punto de referencia": "Punto_de_referencia__c",
    "¿Vive con el niño/a?": "Vive_con_el_nioa__c",
    "¿Cuál es su grado de instrucción?": "Cul_es_su_grado_de_instruccin__c",
    "¿Está laborando actualmente?": "Est_laborando_actualmente__c",
    "¿Cuál es su condición laboral?": "Cul_es_su_condicin_laboral__c",
    "¿Cuántas horas al dia laboras?": "Cuntas_horas_al_dia_laboras__c",
    "¿En qué turno laboras?": "En_qu_turno_laboras__c",

    "¿Deseas agregar un segundo apoderado?": "Deseas_agregar_un_segundo_apoderado__c",

    "Nombre/s del apoderado 2 del niño/a": "Nombres_del_apoderado_2_del_nioa__c",
    "Apellido paterno": "Apellido_paterno_1__c",
    "Apellido materno": "Apellido_Materno1__c",
    "Parentesco.1": "Parentesco1__c",
    "Fecha de nacimiento.1": "Fecha_de_nacimiento1__c",
    "Documento de identidad.1": "Documento_de_identidad1__c",
    "Número de Documento.1": "Nmero_de_Documento1__c",

    "País de nacimiento.1": "Pas_de_nacimiento1__c",
    "Departamento de NACIMIENTO.1": "Departamento_de_NACIMIENTO1__c",
    "Departamento.1": "Departamento1__c",
    "Provincia.1": "Provincia1__c",
    "Distrito.1": "Distrito1__c",
    "Tipo de Zona.1": "Tipo_de_Zona1__c",
    "Dirección Completa": "Direccin_Completa_1__c",
    "Punto de referencia.1": "Punto_de_referencia1__c",
    "¿Cuál es su grado de instrucción?.1": "Cul_es_su_grado_de_instruccin1__c",
    "¿Está laborando actualmente?.1": "Est_laborando_actualmente1__c",
    "¿Cuál es su condición laboral?.1": "Cul_es_su_condicin_laboral1__c",
    "¿Cuántas horas al dia laboras?.1": "Cuntas_horas_al_dia_laboras1__c",
    "¿En qué turno laboras?.1": "En_qu_turno_laboras1__c",

    "¿Con cuántas personas vive el niño/a? (Sin contar al niño/a)": "Con_cuntas_personas_vive_el_nioa_S__c",
    "¿Con quiénes vive el niño/a": "Con_quines_vive_el_nioa__c",
    "¿Cuántas personas son mayores de edad?": "Cuntas_personas_son_mayores_de_edad__c",
    "¿Cuál es el ingreso semanal del hogar en soles?": "Cul_es_el_ingreso_semanal_del_hogar_en__c",
    "¿Cuántas personas aportan en el hogar?": "Cuntas_personas_aportan_en_el_hogar__c",
    "¿Cuántas personas del hogar tienen una cuenta de banco?": "Cuntas_personas_del_hogar_tienen_una_c__c",
    "¿Cuáles son los principales gastos del hogar?": "Cules_son_los_principales_gastos_del_h__c",

    "¿Cuántas habitaciones tienes en total en el hogar?": "Cuntas_habitaciones_tienes_en_total_en__c",
    "¿Cuántas habitaciones son destinadas para dormir?": "Cuntas_habitaciones_son_destinadas_par__c",
    "Tipo de propiedad": "Tipo_de_propiedad__c",
    "El pago de esta propiedad es": "El_pago_de_esta_propiedad_es__c",

    "Cantidad de dispositivos que tienen acceso a internet (celular, laptop, tablet, etc.)":
        "Cantidad_de_dispositivos_que_tienen_acce__c",

    "Principal tipo de acceso a internet en el hogar":
        "Principal_tipo_de_acceso_a_internet_en_e__c",

    "¿El niño/a tiene acceso a un equipo con internet para hacer tareas?":
        "El_nioa_tiene_acceso_a_un_equipo_con__c",

    "En caso de haber seleccionado celular ¿qué operador tiene?":
        "En_caso_de_haber_seleccionado_celular_q__c",

    "¿Cuál es el material predominante del hogar?":
        "Cul_es_el_material_predominante_del_ho__c",

    "¿Con qué servicios básicos cuenta?": "Con_qu_servicios_bsicos_cuenta__c",
    "¿Con qué electrodomésticos cuenta?": "Con_qu_electrodomsticos_cuenta__c",

    "Número de documento del niño": "Nmero_de_documento_del_nio__c",
    "Distrito.2": "Distrito2__c",
    "Distrito.3": "Distrito3__c",

    "ANIO_FUENTE": "ANIO_FUENTE__c"
}

df = df.rename(columns=mapeo)
df = df.drop(columns=["Apellido materno.1", 
                      "Nombre/s del apoderado 1 del niño/a",
                     "Apellido paterno.2",
                      "Apellido paterno.1",
                      "Apellido materno.2"
                     ], errors="ignore")
print("✔ Columnas renombradas para Salesforce")

# ==========================================================
# 5️⃣ Convertir fechas
# ==========================================================
for col in ["Marca_temporal__c", "Fecha_de_nacimiento__c", "Fecha_de_nacimiento1__c"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        df[col] = df[col].apply(lambda x: x.strftime("%Y-%m-%d") if pd.notnull(x) else None)

# ==========================================================
# 6️⃣ Limpiar NaN para Salesforce
# ==========================================================
df = df.replace({np.nan: None, pd.NA: None})

# ==========================================================
# 7️⃣ Convertir todo a string excepto fechas ya tratadas
# ==========================================================
for col in df.columns:
    if col not in ["Marca_temporal__c", "Fecha_de_nacimiento__c", "Fecha_de_nacimiento1__c"]:
        df[col] = df[col].astype(str)

df = df.replace({"nan": None, "None": None, "NaT": None})

# ==========================================================
# 8️⃣ Debug opcional: guardar archivo final para revisar columnas
# ==========================================================
output_debug = Path("data/consolidated/ficha_social/DEBUG_para_salesforce.xlsx")
output_debug.parent.mkdir(parents=True, exist_ok=True)
df.to_excel(output_debug, index=False)
print(f"📁 Archivo final guardado para revisión: {output_debug}")

# ==========================================================
# 9️⃣ Convertir a records
# ==========================================================
records = df.to_dict("records")

# ==========================================================
# 🔟 Insertar en Salesforce
# ==========================================================
job = bulk.create_insert_job(SF_OBJECT, contentType='JSON')

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

print("✅ Proceso enviado a Salesforce")
print("✅ ETL completado y enviado a Salesforce.")
