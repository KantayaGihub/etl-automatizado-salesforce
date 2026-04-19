import os
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

SF_OBJECT = "Formulario_SatisfaccionInicial6grado_e__c"

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

print("=== Iniciando carga a Salesforce (Encuesta Satisfacción Padres) ===")

# ==========================================================
# 3️⃣ Cargar consolidado
# ==========================================================
ruta_consolidado = Path(
    "data/consolidated/encuesta_satisfaccion_padres/Encuesta_Padres_Deduplicado.csv"
)

if not ruta_consolidado.exists():
    raise FileNotFoundError(f"No existe el consolidado: {ruta_consolidado}")

Formulario_Satisfaccion = pd.read_csv(
    ruta_consolidado,
    sep=",",
    dtype={
        "N Documento Apoderado": str,
        "N Documento Niño": str
    }
)

print(f"✔ Archivo leído correctamente: {len(Formulario_Satisfaccion)} filas")

# ==========================================================
# 4️⃣ Limpiar columna de trazabilidad
# ==========================================================
if "ANIO_FUENTE" in Formulario_Satisfaccion.columns:
    Formulario_Satisfaccion = Formulario_Satisfaccion.drop(columns=["ANIO_FUENTE"])

# ==========================================================
# 5️⃣ Convertir fechas
# ==========================================================
for col in ["Marca temporal"]:
    if col in Formulario_Satisfaccion.columns:
        Formulario_Satisfaccion[col] = pd.to_datetime(
            Formulario_Satisfaccion[col],
            errors='coerce',
            dayfirst=True
        )
        Formulario_Satisfaccion[col] = Formulario_Satisfaccion[col].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None
        )

# ==========================================================
# 6️⃣ Convertir métricas numéricas
# ==========================================================
for col in [
    "¿Cuántos hijos tienes en el Programa Kantaya?",
    "¿Cuántos hijos tienes en el Programa Kantaya? "
]:
    if col in Formulario_Satisfaccion.columns:
        Formulario_Satisfaccion[col] = pd.to_numeric(
            Formulario_Satisfaccion[col],
            errors="coerce"
        )

# ==========================================================
# 7️⃣ Renombrar columnas para Salesforce
# ==========================================================
Formulario_Satisfaccion = Formulario_Satisfaccion.rename(columns={

    # Datos de registro
    "Marca temporal": "Marca_temporal__c",
    "Dirección de correo electrónico": "Direccin_de_correo_electrnico__c",

    # Apoderado
    "Nombres del apoderado del niño/a": "Nombres_del_apoderado_del_nioa__c",
    "Apellido paterno": "Apellido_paterno__c",
    "Apellido materno": "Apellido_materno__c",
    "Documento de identidad Apoderado": "Documento_de_identidad__c",
    "N Documento Apoderado": "N_de_documento__c",

    # Niño o niña
    "Nombres del niño/a": "Nombres_del_nioa__c",
    "Apellido paterno ": "Apellido_paterno_1__c",
    "Apellido materno.1": "Apellido_materno1__c",
    "Apellido materno 2": "Apellido_materno_2__c",
    "Documento de identidad Niño": "Documento_de_identidad_1__c",
    "N Documento Niño": "N_de_documento_1__c",

    # Información general del niño/a
    "¿A qué centro Kantaya asiste tu hijo?": "A_qu_centro_Kantaya_asiste_tu_hijo__c",
    "¿Qué profesoras le enseñan a tu hijo?  ": "Qu_profesoras_le_ensean_a_tu_hijo__c",

    # Opiniones y percepciones
    "Mi hijo es motivado por las profesoras Kantaya para ser mejor cada día ": "Mi_hijo_es_motivado_por_las_profesoras_K__c",
    "Mi hijo disfruta aprender con las profesoras Kantaya ": "Mi_hijo_disfruta_aprender_con_las_profes__c",
    "Mi hijo se sintió comprendido por las profesoras Kantaya ": "Mi_hijo_se_sinti_comprendido_por_las_pr__c",
    "Considero que mi hijo mejoró en el colegio desde que asiste al Programa Kantaya ": "Considero_que_mi_hijo_mejor_en_el_coleg__c",
    "Mi hijo expresa mejor sus necesidades, intereses y opiniones desde que asiste al Programa Kantaya  ": "Mi_hijo_expresa_mejor_sus_necesidades_i__c",
    "Mi hijo mejoró en identificar sus emociones desde que asiste al Programa Kantaya ": "Mi_hijo_mejor_en_identificar_sus_emocio__c",
    "Mi hijo es más responsable desde que asiste al Programa Kantaya ": "Mi_hijo_es_ms_responsable_desde_que_asi__c",
    "Mi hijo es más colaborador desde que asiste al Programa Kantaya ": "Mi_hijo_es_ms_colaborador_desde_que_asi__c",
    "Mi hijo asistió feliz a las clases del Programa Kantaya  ": "Mi_hijo_asisti_feliz_a_las_clases_del_P__c",

    "Le cuesta identificar las necesidades de las personas con quienes vive": "Le_cuesta_identificar_las_necesidades_de__c",
    "Se frustra fácilmente al momento de resolver conflictos": "Se_frustra_fcilmente_al_momento_de_reso__c",
    "Reconoce cuando siente emociones como tristeza, enojo, alegría, etc.": "Reconoce_cuando_siente_emociones_como_tr__c",
    "Tiene confianza en sus capacidades y habilidades": "Tiene_confianza_en_sus_capacidades_y_hab__c",
    "Es capaz de hacer sus tareas": "Es_capaz_de_hacer_sus_tareas__c",
    "La escucha y atención de las profesoras": "La_escucha_y_atencin_de_las_profesoras__c",
    "Los contenidos y materiales brindados": "Los_contenidos_y_materiales_brindados__c",
    "El acompañamiento de la organización": "El_acompaamiento_de_la_organizacin__c",
    "La mejora en el aprendizaje de mi/s hijo/s ": "La_mejora_en_el_aprendizaje_de_mis_hijo__c",
    "Considero que mi(s) hijo(s) tienen un mejor aprendizaje gracias a las clases de comunicación, matemática y socioemocional en el Programa Kantaya  ": "Considero_que_mis_hijos_tienen_un_me__c",
    "Considero que mi(s) hijo(s) aprendieron más a través del juego en el Programa Kantaya  ": "Considero_que_mis_hijos_aprendieron__c",
    "Considero que la cantidad de días que mi(s) hijo(s) fueron al Programa Kantaya fueron los adecuados ": "Considero_que_la_cantidad_de_das_que_mi__c",
    "Me sentí escuchado y recibí respuesta de Kantaya ante mis dudas ": "Me_sent_escuchado_y_recib_respuesta_de__c",
    "Me sentí tranquila/o que mi(s) hijo(s) asista(n) al Programa Kantaya  ": "Me_sent_tranquilao_que_mis_hijos_a__c",
    "Recibí comunicaciones oportunas de Kantaya cuando se realizaron eventos o actividades ": "Recib_comunicaciones_oportunas_de_Kanta__c",
    "El Programa Kantaya brindó los materiales educativos necesarios a mi(s) hijo(s) para que aprenda(n) ": "El_Programa_Kantaya_brind_los_materiale__c",
    "El espacio físico del Programa Kantaya fue adecuado para el aprendizaje de mi(s) hijo(s) ": "El_espacio_fsico_del_Programa_Kantaya_f__c",

    "¿Cuántos hijos tienes en el Programa Kantaya?": "Cuntos_hijos_tienes_en_el_Programa_Kan__c",
    "¿Cuántos hijos tienes en el Programa Kantaya? ": "Cuntos_hijos_tienes_en_el_Programa_Kan_1__c",
    "Nombre": "Nombre__c",

    "Expresa sus ideas de manera clara, concisa y firme": "Expresa_sus_ideas_de_manera_clara_conci__c",
    "Solicita ayuda para solucionar los conflictos": "Solicita_ayuda_para_solucionar_los_confl__c",
    "Utiliza la negociación como manera de resolver sus conflictos": "Utiliza_la_negociacin_como_manera_de_re__c",
    "Sabe regular sus emociones (ej: tristeza, enojo, alegría)": "Sabe_regular_sus_emociones_ej_tristeza__c",
    "Trabaja en las tareas que le corresponden hasta finalizarlas": "Trabaja_en_las_tareas_que_le_corresponde__c",
    "Necesita que le recuerden que tiene que hacer sus tareas": "Necesita_que_le_recuerden_que_tiene_que__c",
    "Tiene aprecio por sí mismo y sus capacidades": "Tiene_aprecio_por_s_mismo_y_sus_capacid__c",
    "La contención que le brinda a mi/s hijo/s": "La_contencin_que_le_brinda_a_mis_hijo__c",
    "Considero que la cantidad de días que mi(s) hijo(s) fueron al Programa Kantaya fueron los adecuados .1": "Considero_que_la_cantidad_de_das_que_m_1__c",
    "Recibí respuesta de Kantaya ante mis dudas ": "Recib_respuesta_de_Kantaya_ante_mis_dud__c",
    "Me sentí escuchado por Kantaya cuando tuve dudas ": "Me_sent_escuchado_por_Kantaya_cuando_tu__c",
    "Le cuesta identificar situaciones conflictivas cuando suceden": "Le_cuesta_identificar_situaciones_confli__c",
    "Sabe reconocer las emociones que existen (ej: tristeza, enojo, alegría)": "Sabe_reconocer_las_emociones_que_existen__c",
    "No se detiene en sus intentos cuando no logra un objetivo": "No_se_detiene_en_sus_intentos_cuando_no__c",
    "Le cuesta identificar las capacidades y habilidades que posee": "Le_cuesta_identificar_las_capacidades_y__c",
    "Reconoce el valor de su identidad": "Reconoce_el_valor_de_su_identidad__c",
    "Considero que la cantidad de días que mi(s) hijo(s) fueron al Programa Kantaya fueron los adecuados .1": "Considero_que_la_cantidad_de_das_que_m_1__c",
    "Considero que la cantidad de días que mi(s) hijo(s) fueron al Programa Kantaya fueron los adecuados  2": "Considero_que_la_cantidad_de_das_que_m_1__c",
    "ANIO_FUENTE": "ANIO_FUENTE__c"
})

print("✔ Columnas renombradas para Salesforce")

# ==========================================================
# DEBUG: guardar archivo final con columnas Salesforce
# ==========================================================
output_debug = "data/consolidated/encuesta_satisfaccion_padres/DEBUG_para_salesforce.csv"

Formulario_Satisfaccion.to_csv(
    output_debug,
    index=False,
    encoding="utf-8-sig"
)

print(f"📁 Archivo final guardado: {output_debug}")

# ==========================================================
# 8️⃣ Limpiar nulos para Salesforce
# ==========================================================
Formulario_Satisfaccion = Formulario_Satisfaccion.replace({
    float('nan'): None,
    pd.NA: None,
    np.nan: None
})

# ==========================================================
# 9️⃣ Convertir a lista de diccionarios
# ==========================================================
records = Formulario_Satisfaccion.to_dict('records')

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
