"""Consultar cantidades de registros de Salesforce; no modifica datos."""

import csv
from datetime import datetime
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from simple_salesforce import Salesforce


OBJETOS = (
    ("Asistencia regular", "Asistencias_e7__c", True),
    ("Asistencia extracurricular", "Asistencias_Extracurriculares_e__c", True),
    ("Matrícula", "Matricula_e__c", True),
    ("Ficha Social", "Ficha_social_e__c", True),
    ("Progreso Curricular", "Progreso_curricular_e__c", True),
    ("Solicitud Matrícula", "Formulario_SolicitudMatricula_e__c", True),
    ("Horario regular", "horario_regular__c", True),
    ("Actividades vivenciales", "asistencia_actividades_vivenciales__c", True),
    ("Habilidades", "BD_Habilidades_e__c", True),
    ("Calificaciones", "Calificaciones_areas_e__c", False),
    ("Encuesta de Padres", "Formulario_SatisfaccionInicial6grado_e__c", False),
)


class SesionConTimeout(requests.Session):
    def request(self, method, url, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = 60
        return super().request(method, url, **kwargs)


def conectar():
    nombres = ("SF_USERNAME", "SF_PASSWORD", "SF_SECURITY_TOKEN")
    if any(not os.getenv(nombre) for nombre in nombres):
        raise ValueError("Faltan secrets SF_USERNAME, SF_PASSWORD o SF_SECURITY_TOKEN")
    session = SesionConTimeout()
    # Solo reintentar lecturas; no se habilitan reintentos de escritura.
    session.mount("https://", HTTPAdapter(max_retries=Retry(
        total=3, backoff_factor=1, allowed_methods=frozenset({"GET"}),
        status_forcelist=(429, 500, 502, 503, 504),
    )))
    return Salesforce(username=os.environ[nombres[0]], password=os.environ[nombres[1]],
                      security_token=os.environ[nombres[2]], domain="login",
                      session=session)


def detalle_error(error):
    # No imprimir mensajes remotos ni credenciales: solo tipo y código de error.
    partes = [type(error).__name__]
    status = getattr(error, "status", None)
    if isinstance(status, int):
        partes.append(f"HTTP {status}")
    return "; ".join(partes)


def contar(sf):
    resultados = []
    for proceso, objeto, activo in OBJETOS:
        fila = {"proceso": proceso, "objeto": objeto, "activo_en_etl": activo,
                "registros": None, "estado": "error", "detalle": ""}
        try:
            respuesta = sf.query(f"SELECT COUNT() FROM {objeto}")
            cantidad = respuesta["totalSize"]
            if not isinstance(cantidad, int) or isinstance(cantidad, bool) or cantidad < 0:
                raise ValueError("Respuesta de conteo inválida")
            fila.update(registros=cantidad, estado="correcto")
            print(f"{proceso}: {cantidad:,} registros actuales ({objeto})", flush=True)
        except Exception as error:
            fila["detalle"] = detalle_error(error)
            print(f"{proceso}: ERROR de consulta ({fila['detalle']}); continúa el diagnóstico", flush=True)
        resultados.append(fila)
    return resultados


def guardar_resultados(resultados, destino, error_general=None):
    destino = Path(destino)
    destino.mkdir(parents=True, exist_ok=True)
    fecha = datetime.now(ZoneInfo("America/Lima")).isoformat(timespec="seconds")
    informe = {"fecha_peru": fecha, "error_general": error_general, "objetos": resultados}
    (destino / "resumen.json").write_text(json.dumps(informe, ensure_ascii=False, indent=2) + "\n",
                                          encoding="utf-8")
    columnas = ("proceso", "objeto", "activo_en_etl", "registros", "estado", "detalle")
    with (destino / "resumen.csv").open("w", encoding="utf-8-sig", newline="") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=columnas)
        writer.writeheader()
        writer.writerows(resultados)
    lineas = ["# Cantidades actuales en Salesforce", "", f"Consulta: {fecha} (Perú).", "",
              "Solo lectura. Cantidades visibles para el usuario de integración; excluyen la papelera.",
              "Son registros, no personas únicas. No se descargaron registros individuales.", "",
              "| Proceso | Objeto | Activo en ETL | Registros | Estado |",
              "| --- | --- | --- | ---: | --- |"]
    for fila in resultados:
        cantidad = str(fila["registros"]) if fila["registros"] is not None else "Sin resultado"
        estado = fila["estado"] if not fila["detalle"] else fila["detalle"]
        lineas.append(f"| {fila['proceso']} | `{fila['objeto']}` | "
                      f"{'Sí' if fila['activo_en_etl'] else 'No'} | {cantidad} | {estado} |")
    if error_general:
        lineas.extend(["", f"No fue posible iniciar la consulta: {error_general}."])
    resumen = "\n".join(lineas) + "\n"
    (destino / "resumen.md").write_text(resumen, encoding="utf-8")
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as archivo:
            archivo.write(resumen)


def main():
    resultados = []
    error_general = None
    try:
        resultados = contar(conectar())
    except Exception as error:
        error_general = detalle_error(error)
        print(f"No se pudo iniciar la consulta: {error_general}", flush=True)
    guardar_resultados(resultados, "resultados-salesforce", error_general)
    return 1 if error_general or any(f["estado"] != "correcto" for f in resultados) else 0


if __name__ == "__main__":
    raise SystemExit(main())
