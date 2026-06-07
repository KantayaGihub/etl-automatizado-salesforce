import os
import time
import json
from io import BytesIO

import numpy as np
import pandas as pd

from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO


print("====================================================")
print("=== ETL CARGA SALESFORCE - EXTRACURRICULARES ===")
print("====================================================")


# ================================================================
# CONFIGURACIÓN
# ================================================================
OBJECT_NAME = "Asistencias_Extracurriculares_e__c"

RUTA_CONSOLIDADO = (
    "data/consolidated/asistencias_extracurriculares/"
    "asistencias_extra_consolidado_kantaya.csv"
)

DELETE_BATCH_SIZE = 10000
INSERT_BATCH_SIZE = 1000


# ================================================================
# FUNCIONES AUXILIARES BULK API
# ================================================================
def wait_for_batch(bulk_client, job_id, batch_id):

    while not bulk_client.is_batch_done(batch_id, job_id):

        time.sleep(5)


def get_batch_results_safe(bulk_client, job_id, batch_id):

    try:

        return list(
            bulk_client.get_batch_results(batch_id, job_id)
        )

    except Exception as e:

        print(f"Error obteniendo resultados batch: {e}")

        return []


def summarize_results(results):

    ok = 0
    fail = 0
    errors = {}

    for r in results:

        if isinstance(r, dict):

            success = str(r.get("success", "")).lower() == "true"
            err = r.get("errors")

        else:

            success = str(getattr(r, "success", False)).lower() == "true"
            err = getattr(r, "error", None)

            if err is None:
                err = getattr(r, "errors", None)

        if success:

            ok += 1

        else:

            fail += 1

            if err is None:
                err = "Sin detalle"

            if isinstance(err, list):
                err = " | ".join([str(x) for x in err])

            err = str(err)
            errors[err] = errors.get(err, 0) + 1

    return ok, fail, errors


# ================================================================
# VARIABLES DE ENTORNO
# ================================================================
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


# ================================================================
# CONEXIÓN SALESFORCE  —  UNA SOLA INSTANCIA para todo el script
# ================================================================
print("\n=== CONECTANDO A SALESFORCE ===")

sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain="login"
)

bulk = SalesforceBulk(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    sandbox=False
)

print("Conexión Salesforce OK")


# ================================================================
# ELIMINACIÓN PREVIA
# ================================================================
print("\n=== ELIMINANDO REGISTROS PREVIOS ===")

query = f"SELECT Id FROM {OBJECT_NAME}"

records_delete = sf.query_all(query)["records"]

if not records_delete:

    print("No existen registros previos")

else:

    ids = [{"Id": r["Id"]} for r in records_delete]

    print(f"Registros encontrados para eliminar: {len(ids):,}")

    delete_job = bulk.create_delete_job(OBJECT_NAME, contentType="JSON")

    delete_batches = []

    for i in range(0, len(ids), DELETE_BATCH_SIZE):

        batch_records = ids[i:i + DELETE_BATCH_SIZE]

        json_bytes = json.dumps(batch_records).encode("utf-8")

        batch_id = bulk.post_batch(
            delete_job,
            IteratorBytesIO(iter([json_bytes]))
        )

        delete_batches.append(batch_id)

        print(f"Batch delete enviado {i // DELETE_BATCH_SIZE + 1}")

    bulk.close_job(delete_job)

    # ----------------------------------------------------------------
    # Esperar a que TODOS los batches de delete terminen
    # antes de continuar con el insert
    # ----------------------------------------------------------------
    print("Esperando finalización de deletes...")

    for batch_id in delete_batches:

        wait_for_batch(bulk, delete_job, batch_id)

    print("Eliminación completada")


# ================================================================
# CARGAR CONSOLIDADO
# ================================================================
print("\n=== CARGANDO CONSOLIDADO ===")

Asistencias = pd.read_csv(
    RUTA_CONSOLIDADO,
    sep=",",
    dtype={"DNI": str}
)

print("CSV cargado correctamente")
print(f"Filas: {len(Asistencias):,}")
print(f"Columnas: {len(Asistencias.columns)}")


# ================================================================
# VALIDACIONES QA
# ================================================================
print("\n=== VALIDACIONES QA ===")

dup_cols = [c for c in ["DNI", "FECHA"] if c in Asistencias.columns]

if dup_cols:

    dups = Asistencias.duplicated(subset=dup_cols).sum()
    print(f"Duplicados {dup_cols}: {dups:,}")

for c in ["DNI", "FECHA", "ASISTENCIA"]:

    if c in Asistencias.columns:

        nulos = Asistencias[c].isna().sum()
        print(f"Nulos {c}: {nulos:,} / {len(Asistencias):,}")

if "CENTRO" in Asistencias.columns:

    print("\n=== TOP CENTROS ===")
    print(Asistencias["CENTRO"].value_counts(dropna=False).head(10))


# ================================================================
# FECHAS
# ================================================================
print("\n=== TRANSFORMANDO FECHAS ===")

for col in ["F_INCORPORACION", "F_SALIDA", "FECHA"]:

    if col in Asistencias.columns:

        Asistencias[col] = pd.to_datetime(
            Asistencias[col],
            errors="coerce",
            dayfirst=True
        )

        Asistencias[col] = Asistencias[col].apply(
            lambda x: x.strftime("%Y-%m-%d") if pd.notnull(x) else None
        )

print("Fechas transformadas")


# ================================================================
# NUMÉRICOS
# ================================================================
print("\n=== TRANSFORMANDO NUMÉRICOS ===")

for col in ["A_ESPERADAS", "A_REALES", "PORC_PART"]:

    if col in Asistencias.columns:

        Asistencias[col] = pd.to_numeric(Asistencias[col], errors="coerce")

print("Numéricos transformados")


# ================================================================
# LIMPIEZA CENTRO
# ================================================================
print("\n=== LIMPIANDO CENTRO ===")

if "CENTRO" in Asistencias.columns:

    Asistencias["CENTRO"] = (
        Asistencias["CENTRO"]
        .astype(str)
        .str.replace(" REGISTRO DE ASISTENCIA 2025.xlsx", "", regex=False)
        .str.replace(" REGISTRO DE ASISTENCIA 2026.xlsx", "", regex=False)
        .str.replace("_", " ", regex=False)
    )

    Asistencias["CENTRO"] = Asistencias["CENTRO"].replace("nan", None)

print("Centro limpio")


# ================================================================
# NORMALIZAR NULOS
# ================================================================
print("\n=== NORMALIZANDO NULOS ===")

Asistencias = Asistencias.where(pd.notnull(Asistencias), None)

Asistencias = Asistencias.replace({
    float("nan"): None,
    pd.NA: None,
    np.nan: None
})

print("Nulos normalizados")


# ================================================================
# RENOMBRAR COLUMNAS
# ================================================================
print("\n=== PREPARANDO COLUMNAS SALESFORCE ===")

Asistencias.columns = [col + "__c" for col in Asistencias.columns]


# ================================================================
# VALIDAR CAMPOS SALESFORCE
# ================================================================
print("\n=== VALIDANDO CAMPOS SALESFORCE ===")

sf_fields = {
    f["name"]
    for f in sf.Asistencias_Extracurriculares_e__c.describe()["fields"]
}

invalid_cols = [c for c in Asistencias.columns if c not in sf_fields]

if invalid_cols:

    print("\n Campos inexistentes en Salesforce:")
    print(invalid_cols)
    Asistencias = Asistencias.drop(columns=invalid_cols)

print("Validación de campos OK")


# ================================================================
# CONVERTIR A RECORDS
# ================================================================
records_insert = Asistencias.to_dict("records")

print("\n=== RESUMEN FINAL PRE-CARGA ===")
print(f"Registros a insertar: {len(records_insert):,}")
print(f"Campos finales: {len(Asistencias.columns)}")


# ================================================================
# INSERT BULK API  —  reutiliza la misma instancia `bulk`
# ================================================================
print("\n=== ENVIANDO A SALESFORCE BULK API ===")

# NOTA: NO se re-instancia SalesforceBulk aquí.
# Se usa la misma variable `bulk` creada al inicio del script
# para garantizar que el DELETE ya finalizó antes del INSERT.

insert_job = bulk.create_insert_job(OBJECT_NAME, contentType="JSON")

insert_batches = []

for i in range(0, len(records_insert), INSERT_BATCH_SIZE):

    batch_records = records_insert[i:i + INSERT_BATCH_SIZE]

    json_data = json.dumps(batch_records, ensure_ascii=False, allow_nan=False)

    batch_io = BytesIO(json_data.encode("utf-8"))

    batch_id = bulk.post_batch(insert_job, batch_io)

    insert_batches.append(batch_id)

    print(
        f"Lote {i // INSERT_BATCH_SIZE + 1} enviado "
        f"({len(batch_records)} registros)"
    )

bulk.close_job(insert_job)

print("\n Esperando resultados Salesforce...")


# ================================================================
# RESULTADOS
# ================================================================
insertados = 0
fallidos = 0
errores_totales = {}

for i, batch_id in enumerate(insert_batches, start=1):

    wait_for_batch(bulk, insert_job, batch_id)

    resultados = get_batch_results_safe(bulk, insert_job, batch_id)

    ok, fail, errores = summarize_results(resultados)

    insertados += ok
    fallidos += fail

    for err, n in errores.items():
        errores_totales[err] = errores_totales.get(err, 0) + n

    print(f"Lote {i}: insertados={ok}, fallidos={fail}")


# ================================================================
# RESUMEN FINAL
# ================================================================
print("\n====================================================")
print("=== RESULTADO FINAL ===")
print("====================================================")

print(f"Insertados: {insertados:,}")
print(f"Fallidos: {fallidos:,}")
print(f"Total procesados: {len(records_insert):,}")

if errores_totales:

    print("\n=== TOP ERRORES SALESFORCE ===")

    for err, n in sorted(
        errores_totales.items(),
        key=lambda x: x[1],
        reverse=True
    )[:10]:

        print(f"{n} filas -> {err}")

print("\n PROCESO FINALIZADO")
