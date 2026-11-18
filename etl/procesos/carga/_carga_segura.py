"""Preparación y reemplazo explícito sin respaldo de datos de Salesforce."""

import argparse
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from io import BytesIO
import json
import os
from pathlib import Path
import time

import pandas as pd
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk


PREPARADOS = Path("data/prepared")
RESULTADOS = Path("data/load-results.json")
# Excepción explícita: conservar estos nombres originales en el envío de solicitud.
CAMPOS_LEGADOS = {
    "Formulario_SolicitudMatricula_e__c": {"Edad actual", "Edad de ingreso"},
}
OBJETOS = (
    "Asistencias_e7__c", "Asistencias_Extracurriculares_e__c", "Matricula_e__c",
    "Calificaciones_areas_e__c", "Formulario_SatisfaccionInicial6grado_e__c",
    "Ficha_social_e__c", "Progreso_curricular_e__c", "Formulario_SolicitudMatricula_e__c",
    "horario_regular__c", "asistencia_actividades_vivenciales__c", "BD_Habilidades_e__c",
)


def guardar_json(ruta, datos):
    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    temporal = ruta.with_suffix(ruta.suffix + ".tmp")
    temporal.write_text(json.dumps(datos, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    temporal.replace(ruta)


def conectar():
    nombres = ("SF_USERNAME", "SF_PASSWORD", "SF_SECURITY_TOKEN")
    if any(not os.getenv(n) for n in nombres):
        raise ValueError("Faltan credenciales de Salesforce")
    return Salesforce(username=os.environ[nombres[0]], password=os.environ[nombres[1]],
                      security_token=os.environ[nombres[2]], domain="login")


def validar_registros(registros, campos, objeto=None):
    if not registros:
        raise ValueError("No se permite reemplazar Salesforce con cero registros")
    metadata = {c["name"]: c for c in campos}
    excepciones = CAMPOS_LEGADOS.get(objeto, set())
    advertidos = set()
    requeridos = {c["name"] for c in campos if c.get("createable")
                  and not c.get("nillable") and not c.get("defaultedOnCreate")}
    for numero, registro in enumerate(registros, 1):
        if not isinstance(registro, dict) or not registro:
            raise ValueError(f"Registro {numero} sin campos")
        desconocidos = set(registro) - set(metadata)
        no_escribibles = {n for n in registro if n in metadata and not metadata[n].get("createable")}
        advertidos.update((desconocidos | no_escribibles) & excepciones)
        desconocidos -= excepciones
        no_escribibles -= excepciones
        if desconocidos or no_escribibles:
            raise ValueError("Campos desconocidos o no insertables: "
                             + ", ".join(sorted(desconocidos | no_escribibles)))
        vacios = {n for n in requeridos if registro.get(n) is None or registro.get(n) == ""}
        if vacios:
            raise ValueError(f"Registro {numero}: faltan campos obligatorios: " + ", ".join(sorted(vacios)))
        for nombre, valor in registro.items():
            if nombre in excepciones and (
                    nombre not in metadata or not metadata[nombre].get("createable")):
                continue
            if valor is None:
                continue
            limite = metadata[nombre].get("length", 0)
            if isinstance(valor, str) and limite and len(valor) > limite:
                raise ValueError(f"Registro {numero}: longitud excedida en {nombre}")
            tipo = metadata[nombre].get("type")
            if tipo in ("int", "double", "currency", "percent"):
                try:
                    decimal = Decimal(str(valor))
                    if not decimal.is_finite() or (tipo == "int" and decimal != decimal.to_integral_value()):
                        raise ValueError()
                except (InvalidOperation, ValueError):
                    raise ValueError(f"Registro {numero}: valor no numérico en {nombre}") from None
            if tipo == "boolean" and not isinstance(valor, bool):
                raise ValueError(f"Registro {numero}: booleano inválido en {nombre}")
            if tipo == "date":
                try:
                    date.fromisoformat(str(valor))
                except ValueError:
                    raise ValueError(f"Registro {numero}: fecha inválida en {nombre}") from None
            if tipo == "datetime":
                try:
                    datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
                except ValueError:
                    raise ValueError(f"Registro {numero}: fecha/hora inválida en {nombre}") from None
            if metadata[nombre].get("restrictedPicklist"):
                permitidos = {v["value"] for v in metadata[nombre].get("picklistValues", []) if v.get("active")}
                seleccionados = str(valor).split(";") if tipo == "multipicklist" else [valor]
                if any(v not in permitidos for v in seleccionados):
                    raise ValueError(f"Registro {numero}: opción inválida en {nombre}")
    json.dumps(registros, ensure_ascii=False, allow_nan=False)
    if advertidos:
        print(f"AVISO {objeto}: se conservarán en el envío los campos sin homologar: "
              + ", ".join(sorted(advertidos))
              + ". Su aceptación se comprobará en los resultados de Salesforce.")


def preparar(sf, objeto, dataframe):
    """Solo lectura remota y salida local. Nunca borra ni inserta registros."""
    try:
        return _preparar(sf, objeto, dataframe)
    except ValueError as error:
        mensaje = str(error).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        if os.getenv("ETL_PUBLICAR_ERRORES") == "true":
            print(f"::error title=Prevalidación de {objeto}::{mensaje}", flush=True)
        raise


def _preparar(sf, objeto, dataframe):
    if objeto not in OBJETOS:
        raise ValueError("Objeto fuera de la lista de cargas")
    if dataframe.empty:
        raise ValueError(f"{objeto}: consolidado sin filas")
    if dataframe.columns.duplicated().any():
        duplicadas = dataframe.columns[dataframe.columns.duplicated()].tolist()
        raise ValueError(f"{objeto}: columnas duplicadas después del mapeo: {duplicadas}")
    registros = dataframe.to_dict("records")
    campos = getattr(sf, objeto).describe()["fields"]
    validar_registros(registros, campos, objeto)
    guardar_json(PREPARADOS / f"{objeto}.json", {"objeto": objeto, "registros": registros})
    print(f"PREPARADO {objeto}: {len(registros)} registros; Salesforce sin modificar")


def leer_preparados():
    archivos = sorted(PREPARADOS.glob("*.json"))
    if not archivos:
        raise ValueError("No hay registros preparados")
    preparados = {}
    for ruta in archivos:
        contenido = json.loads(ruta.read_text(encoding="utf-8"))
        objeto = contenido["objeto"]
        if objeto not in OBJETOS or ruta.name != f"{objeto}.json" or objeto in preparados:
            raise ValueError("Archivo preparado inesperado")
        preparados[objeto] = (ruta, contenido["registros"])
    return {objeto: preparados[objeto] for objeto in OBJETOS if objeto in preparados}


def comprobar_preparados(sf):
    preparados = leer_preparados()
    # Validar todos los objetos antes de crear cualquier job de escritura.
    for objeto, (_, registros) in preparados.items():
        validar_registros(registros, getattr(sf, objeto).describe()["fields"], objeto)
    # Solo IDs para el reemplazo; no se exportan registros ni se genera backup.
    anteriores = {
        objeto: sf.query_all(f"SELECT Id FROM {objeto}")["records"]
        for objeto in preparados
    }
    return preparados, anteriores


def ejecutar_lotes(bulk, objeto, operacion, registros, informe, timeout=600):
    crear = bulk.create_delete_job if operacion == "delete" else bulk.create_insert_job
    job = crear(objeto, contentType="JSON")
    informe.setdefault("jobs", []).append({"operacion": operacion, "objeto": objeto, "job": job})
    guardar_json(RESULTADOS, informe)
    lotes = []
    try:
        for inicio in range(0, len(registros), 1000):
            lote = registros[inicio:inicio + 1000]
            batch = bulk.post_batch(job, BytesIO(json.dumps(lote, ensure_ascii=False, allow_nan=False).encode("utf-8")))
            lotes.append((batch, len(lote)))
            informe.setdefault("lotes", []).append({"operacion": operacion, "job": job, "batch": batch})
            guardar_json(RESULTADOS, informe)
        bulk.close_job(job)
    except Exception:
        try:
            bulk.abort_job(job)
        except Exception:
            informe["abort_pendiente"] = job
            guardar_json(RESULTADOS, informe)
        raise
    ids = []
    for batch, esperados in lotes:
        limite = time.monotonic() + timeout
        while not bulk.is_batch_done(batch, job):
            if time.monotonic() >= limite:
                raise TimeoutError(f"Tiempo agotado en {operacion}; revisar el job remoto antes de reintentar")
            time.sleep(5)
        resultados = list(bulk.get_batch_results(batch, job))
        def valor(r, nombre):
            if isinstance(r, dict):
                return r.get(nombre, r.get("error") if nombre == "errors" else None)
            if nombre == "errors":
                return getattr(r, "errors", getattr(r, "error", None))
            return getattr(r, nombre, None)
        correctos = [r for r in resultados if str(valor(r, "success")).lower() == "true"]
        informe.setdefault("resultados", []).append({
            "operacion": operacion, "objeto": objeto, "batch": batch,
            "correctos": len(correctos), "esperados": esperados,
            "ids": [valor(r, "id") for r in correctos],
            "errores": [str(valor(r, "errors")) for r in resultados
                        if str(valor(r, "success")).lower() != "true"][:20],
        })
        guardar_json(RESULTADOS, informe)
        if len(resultados) != esperados or len(correctos) != esperados:
            raise RuntimeError(f"{objeto}: {operacion} incompleto; revisar lotes del informe")
        ids.extend(valor(r, "id") for r in correctos)
    return ids


def cargar(sf, bulk):
    preparados, existentes = comprobar_preparados(sf)
    informe = {"estado": "en curso", "objetos_completados": []}
    guardar_json(RESULTADOS, informe)
    try:
        for objeto, (_, registros) in preparados.items():
            anteriores = existentes[objeto]
            # Con campos legados, comprobar la inserción antes de borrar los datos anteriores.
            insertar_primero = any(CAMPOS_LEGADOS.get(objeto, set()) & set(r) for r in registros)
            if insertar_primero:
                ids = ejecutar_lotes(bulk, objeto, "insert", registros, informe)
            if anteriores:
                ejecutar_lotes(bulk, objeto, "delete", [{"Id": r["Id"]} for r in anteriores], informe)
            if not insertar_primero:
                ids = ejecutar_lotes(bulk, objeto, "insert", registros, informe)
            actuales = sf.query_all(f"SELECT Id FROM {objeto}")["records"]
            if len(ids) != len(registros) or {r["Id"] for r in actuales} != set(ids):
                raise RuntimeError(f"{objeto}: no coincide la verificación posterior a la carga")
            informe["objetos_completados"].append(objeto)
            guardar_json(RESULTADOS, informe)
            print(f"VERIFICADO {objeto}: {len(ids)} insertados")
        informe["estado"] = "completado"
    except Exception as error:
        informe.update(estado="error", error=type(error).__name__)
        raise
    finally:
        guardar_json(RESULTADOS, informe)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operacion", choices=("cargar",))
    operacion = parser.parse_args().operacion
    sf = conectar()
    # Solo se crea el cliente Bulk para la operación explícita de carga.
    bulk = SalesforceBulk(username=os.environ["SF_USERNAME"], password=os.environ["SF_PASSWORD"],
                          security_token=os.environ["SF_SECURITY_TOKEN"], sandbox=False)
    cargar(sf, bulk)
