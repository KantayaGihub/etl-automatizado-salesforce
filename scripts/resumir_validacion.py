"""Inventariar salidas de la validación; no conecta a servicios externos."""

import json
import os
from pathlib import Path

import pandas as pd


def generar_resumen(raiz, pasos):
    raiz = Path(raiz)
    destino = raiz / "resultados-validacion"
    destino.mkdir(parents=True, exist_ok=True)
    fallidos = [nombre for nombre, estado in pasos.items()
                if nombre.startswith("paso_") and estado.get("outcome") != "success"]
    archivos = []
    for carpeta in ("processed", "consolidated"):
        for archivo in sorted((raiz / "data" / carpeta).rglob("*")):
            if archivo.suffix.lower() not in (".csv", ".xlsx") or "CALIDAD" in archivo.name:
                continue
            registro = {"archivo": str(archivo.relative_to(raiz)), "filas": None, "error": None}
            try:
                df = pd.read_csv(archivo) if archivo.suffix == ".csv" else pd.read_excel(archivo)
                registro["filas"] = len(df)
                if df.empty:
                    registro["error"] = "Sin registros"
            except Exception as error:
                # Solo registrar el tipo: evitar copiar datos de filas a los informes.
                registro["error"] = type(error).__name__
            archivos.append(registro)

    incompleto = not archivos or bool(fallidos) or any(a["error"] for a in archivos)
    informe = {"pasos": pasos, "pasos_no_exitosos": fallidos, "archivos": archivos,
               "con_errores": incompleto}
    (destino / "resumen.json").write_text(json.dumps(informe, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    lineas = ["# Validación ETL 2025 y 2026 sin Salesforce", "",
              "Se ejecutaron únicamente descargas, transformaciones y consolidaciones.",
              "Los conteos no certifican que las filas sean válidas para cargar a Salesforce.", "",
              "## Estado de pasos", "", "| Paso | Resultado |", "| --- | --- |"]
    for nombre, estado in pasos.items():
        if nombre.startswith("paso_"):
            lineas.append(f"| {nombre} | {estado.get('outcome', 'desconocido')} |")
    lineas.extend(["", "## Archivos procesados y consolidados", "",
                   "| Archivo | Filas | Error |", "| --- | ---: | --- |"])
    for a in archivos:
        lineas.append(f"| {a['archivo']} | {a['filas']} | {a['error'] or ''} |")
    if not archivos:
        lineas.extend(["", "No se generaron archivos procesados o consolidados."])
    lineas.extend(["", "Resultado: " + ("revisar errores" if incompleto else "pasos y lecturas completados")])
    (destino / "resumen.md").write_text("\n".join(lineas) + "\n", encoding="utf-8")
    return incompleto


if __name__ == "__main__":
    fallos = generar_resumen(Path.cwd(), json.loads(os.environ.get("PASOS_JSON", "{}")))
    salida = os.environ.get("GITHUB_OUTPUT")
    if salida:
        with open(salida, "a", encoding="utf-8") as archivo:
            archivo.write(f"con_errores={'true' if fallos else 'false'}\n")
