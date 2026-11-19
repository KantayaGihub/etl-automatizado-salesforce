"""Exportar Matrícula y Solicitud Matrícula actuales a Excel, sin modificar Salesforce."""

from datetime import datetime
import json
import os
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from openpyxl import Workbook
from openpyxl.comments import Comment
from openpyxl.styles import Font, PatternFill
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

from contar_salesforce import conectar, detalle_error


OBJETOS = (
    ("Matricula", "Matricula_e__c"),
    ("Solicitud_Matricula", "Formulario_SolicitudMatricula_e__c"),
)
SALIDA = Path("resultados-salesforce/datos_matricula_salesforce.xlsx")


def valor_excel(valor):
    if isinstance(valor, (dict, list)):
        valor = json.dumps(valor, ensure_ascii=False)
    if isinstance(valor, str) and (len(valor) > 32767 or ILLEGAL_CHARACTERS_RE.search(valor)):
        raise ValueError("Un valor no se puede representar íntegramente en una celda Excel")
    return valor


def exportar(sf, destino):
    libro = Workbook()
    libro.remove(libro.active)
    resumen = []
    for hoja, objeto in OBJETOS:
        descripcion = getattr(sf, objeto).describe()
        campos = [campo for campo in descripcion["fields"] if not campo.get("deprecatedAndHidden")]
        nombres = [campo["name"] for campo in campos]
        if not nombres or len(nombres) > 16384 or any(not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", n) for n in nombres):
            raise ValueError("Metadata de campos no válida para la exportación")
        # query_all recorre la paginación. Sin LIMIT ni filtros de año o de DNI.
        consulta = sf.query_all(f"SELECT {','.join(nombres)} FROM {objeto} ORDER BY Id")
        registros = consulta["records"]
        if consulta["totalSize"] != len(registros) or len(registros) > 1048575:
            raise ValueError("La cantidad de registros no permite una exportación completa a Excel")
        tabla = libro.create_sheet(hoja)
        for columna, campo in enumerate(campos, 1):
            celda = tabla.cell(1, columna, campo["name"])
            celda.comment = Comment(campo.get("label", campo["name"]), "Salesforce")
            celda.font = Font(bold=True, color="FFFFFF")
            celda.fill = PatternFill("solid", fgColor="245A81")
        for fila, registro in enumerate(registros, 2):
            for columna, nombre in enumerate(nombres, 1):
                valor = valor_excel(registro.get(nombre))
                celda = tabla.cell(fila, columna, valor)
                if isinstance(valor, str):
                    # Conservar DNI con ceros iniciales y textos que empiezan por '=' como texto.
                    celda.data_type = "s"
                    celda.number_format = "@"
        tabla.freeze_panes = "A2"
        tabla.auto_filter.ref = tabla.dimensions
        for columna in tabla.iter_cols(min_row=1, max_row=1):
            tabla.column_dimensions[columna[0].column_letter].width = 28
        resumen.append({"hoja": hoja, "objeto": objeto, "registros": len(registros), "columnas": len(nombres)})
    # Publicar solo cuando ambas hojas estén completas.
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_name(destino.stem + ".tmp.xlsx")
    libro.save(temporal)
    temporal.replace(destino)
    return resumen


def main():
    fecha = datetime.now(ZoneInfo("America/Lima")).isoformat(timespec="seconds")
    lineas = ["# Excel de Matrícula y Solicitud Matrícula", "", f"Consulta: {fecha} (Perú).", "",
              "Registros actuales de Salesforce visibles para el usuario de integración, sin papelera.",
              "Incluye todos los campos disponibles, incluidos Id y campos de sistema.",
              "No ejecuta el ETL ni modifica Salesforce. Los valores personales están solo en el Excel.", ""]
    estado = 0
    try:
        resumen = exportar(conectar(), SALIDA)
        lineas.extend(["| Hoja | Objeto | Registros | Columnas |", "| --- | --- | ---: | ---: |"])
        for fila in resumen:
            mensaje = f"{fila['registros']} registros y {fila['columnas']} columnas ({fila['objeto']})"
            print(f"{fila['hoja']}: {mensaje}", flush=True)
            if os.getenv("SF_PUBLICAR_CONTEOS") == "true":
                print(f"::notice title=Excel {fila['hoja']}::{mensaje}", flush=True)
            lineas.append(f"| {fila['hoja']} | `{fila['objeto']}` | {fila['registros']} | {fila['columnas']} |")
        lineas.extend(["", f"Archivo: `{SALIDA.name}`. Descargar el artefacto de esta ejecución."])
    except Exception as error:
        estado = 1
        detalle = detalle_error(error)
        print(f"No se generó el Excel completo: {detalle}", flush=True)
        if os.getenv("SF_PUBLICAR_CONTEOS") == "true":
            print(f"::error title=Exportacion de solo lectura::{detalle}", flush=True)
        lineas.append(f"Error: {detalle}. No se publicó un Excel parcial.")
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as archivo:
            archivo.write("\n".join(lineas) + "\n")
    return estado


if __name__ == "__main__":
    raise SystemExit(main())
