from _carga_segura import preparar
import os

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce


print("================================================")
print("=== ETL PREPARACION SALESFORCE - MATRICULA ===")
print("================================================")


OBJECT_NAME = "Matricula_e__c"
INPUT_PATH = "data/consolidated/matricula/consolidado_matricula_afterschool.csv"
OUTPUT_FINAL = "data/consolidated/matricula/matricula_final_para_salesforce.csv"


print("\n=== VALIDANDO VARIABLES DE ENTORNO ===")

SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")

if not SF_USERNAME:
    raise ValueError("Falta SF_USERNAME")
if not SF_PASSWORD:
    raise ValueError("Falta SF_PASSWORD")
if not SF_SECURITY_TOKEN:
    raise ValueError("Falta SF_SECURITY_TOKEN")

print("Variables cargadas correctamente")


print("\n=== CONECTANDO A SALESFORCE ===")

sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain="login"
)


print("Conexion Salesforce OK")


print("\n=== CARGANDO CONSOLIDADO ===")

Matricula = pd.read_csv(
    INPUT_PATH,
    sep=",",
    dtype={"DNI": str, "DNI DEL NIÑO": str, "DNI DEL NINO": str}
)

print(f"Archivo leido correctamente: {len(Matricula)} filas")


# ================================================================
# LIMPIEZA Y TRANSFORMACION
# ================================================================

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

if "ANIO_FUENTE" in Matricula.columns:
    Matricula = Matricula.drop(columns=["ANIO_FUENTE"])

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

Matricula = Matricula.where(pd.notnull(Matricula), None)

Matricula.columns = [col + "__c" for col in Matricula.columns]

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

print("Archivo preparado correctamente para Salesforce Bulk API")


Matricula = Matricula.replace({float('nan'): None, pd.NA: None, np.nan: None})

Matricula.to_csv(OUTPUT_FINAL, index=False, encoding="utf-8-sig")
preparar(sf, OBJECT_NAME, Matricula)
