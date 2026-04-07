import csv
import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd


INPUT_FILE = Path("data/raw/2025/4.calificaciones_area_competencia/comparativa_ventanilla_proyectos.xlsx")
OUTPUT_DIR = Path("data/processed/2025/4.calificaciones_area_competencia")
OUTPUT_FILE = OUTPUT_DIR / "BD_Promedios_Areas_LB_LS_Consolidado.csv"

YEAR_SOURCE_COL = "PERMANENCIA"
ANIO_PROYECTO = None


def normalizar_texto(texto):
    if pd.isna(texto):
        return texto
    texto = str(texto)
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("utf-8")
    return texto.strip().upper()


def limpiar_registros(df):
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"[\n\r\t;]+", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        df.loc[df[col].isin(["nan", "None", "NaT"]), col] = np.nan
    return df


def cargar_equivalencias(xls):
    df_eq = pd.read_excel(
        xls,
        sheet_name="Equivalencias Competencias",
        header=2
    )

    df_eq = df_eq.rename(columns={
        "Área": "AREA_EVALUACION",
        "Equivalencia": "COMPETENCIA",
        "Competencia": "COMPETENCIA_TEXTO"
    })

    df_eq = df_eq.dropna(subset=["AREA_EVALUACION", "COMPETENCIA"])

    df_eq["AREA_EVALUACION"] = df_eq["AREA_EVALUACION"].astype(str).str.strip()
    df_eq["COMPETENCIA"] = df_eq["COMPETENCIA"].astype(str).str.strip()
    df_eq["COMPETENCIA_TEXTO"] = df_eq["COMPETENCIA_TEXTO"].astype(str).str.strip()

    return df_eq


def extraer_anio(df, col_name):
    if col_name not in df.columns:
        return np.nan

    valores = df[col_name].dropna().astype(str).unique()
    anios = []

    for v in valores:
        encontrados = re.findall(r"(\d{4})", v)
        for e in encontrados:
            anios.append(int(e))

    return max(anios) if anios else np.nan


def preparar_df(df):
    df2 = df.copy()

    df2.columns = (
        df2.columns
        .str.replace(r"[\n\r\t;]+", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.upper()
    )

    df2 = df2.loc[:, ~df2.columns.duplicated()]
    return df2


def procesar_hoja(df, area_nombre, momento):
    df2 = preparar_df(df)

    if "DNI" not in df2.columns:
        print(f"No se encontró DNI en hoja {area_nombre} {momento}")
        return None

    metadata_cols = [
        "DNI",
        "APELLIDOS Y NOMBRES",
        "GRADO",
        "SEXO",
        "CENTRO",
        "CONDICIÓN ACTUAL",
        "TOTAL PREGUNTAS ACERTADAS",
    ]
    metadata_cols = [c for c in metadata_cols if c in df2.columns]

    df_meta = df2[metadata_cols].copy()

    cols_prom = [c for c in df2.columns if re.search(r"PROMEDIO\s*C\d+", c)]
    cols_logro = [c for c in df2.columns if re.search(r"NIVEL DE LOGRO\s*C\d+", c)]

    if not cols_prom or not cols_logro:
        return None

    df_prom = df2.melt(
        id_vars="DNI",
        value_vars=cols_prom,
        var_name="col",
        value_name="VALOR_PROMEDIO"
    )
    df_prom["COMPETENCIA"] = df_prom["col"].str.extract(r"(C\d+)")

    df_logro = df2.melt(
        id_vars="DNI",
        value_vars=cols_logro,
        var_name="col",
        value_name="VALOR_LOGRO"
    )
    df_logro["COMPETENCIA"] = df_logro["col"].str.extract(r"(C\d+)")

    df_merge = pd.merge(
        df_prom[["DNI", "COMPETENCIA", "VALOR_PROMEDIO"]],
        df_logro[["DNI", "COMPETENCIA", "VALOR_LOGRO"]],
        on=["DNI", "COMPETENCIA"],
        how="inner"
    )

    col_pct = [c for c in df2.columns if "PORCENTAJE DE PROMEDIO" in c]
    col_nivel_general = [c for c in df2.columns if c.strip() == "NIVEL DE LOGRO"]

    if col_pct:
        df_meta["PORCENTAJE_PROMEDIO"] = df2[col_pct[0]]

    if col_nivel_general:
        df_meta["NIVEL_LOGRO_GENERAL"] = df2[col_nivel_general[0]]

    df_final = pd.merge(df_merge, df_meta, on="DNI", how="left")
    df_final["AREA_EVALUACION"] = area_nombre
    df_final["MOMENTO"] = momento

    return df_final


def cargar_y_procesar(xls, sheet_name, area_nombre, momento):
    global ANIO_PROYECTO

    df_temp = pd.read_excel(xls, sheet_name=sheet_name, header=None)

    header_row = None
    for i in range(len(df_temp)):
        fila = df_temp.iloc[i].astype(str).str.upper()
        if fila.str.contains("DNI").any():
            header_row = i
            break

    if header_row is None:
        print(f"No se encontró encabezado con DNI en hoja: {sheet_name}")
        return None

    df_raw = pd.read_excel(
        xls,
        sheet_name=sheet_name,
        header=header_row,
        dtype=str
    )

    df_raw = limpiar_registros(df_raw)

    if momento == "LB":
        for c in df_raw.columns:
            if c.upper() == YEAR_SOURCE_COL:
                ANIO_PROYECTO = extraer_anio(df_raw, c)
                break

    df_proc = procesar_hoja(df_raw, area_nombre, momento)

    if df_proc is None:
        return None

    df_proc["ANIO"] = ANIO_PROYECTO
    return df_proc


def transformar_archivo(input_file: Path) -> pd.DataFrame:
    if not input_file.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {input_file}")

    xls = pd.ExcelFile(input_file)
    df_equivalencias = cargar_equivalencias(xls)

    sheets = [
        ("LB_Matemática", "Matemática", "LB"),
        ("LS_Matemática", "Matemática", "LS"),
        ("LB_Comunicación", "Comunicación", "LB"),
        ("LS_Comunicación", "Comunicación", "LS"),
        ("LB_Socioemocional", "Socioemocional", "LB"),
        ("LS_Socioemocional", "Socioemocional", "LS"),
        ("LB_Tecnología", "Tecnología", "LB"),
        ("LS_Tecnología", "Tecnología", "LS"),
        ("LB_Ciencias", "Ciencias", "LB"),
        ("LS_Ciencias", "Ciencias", "LS"),
    ]

    dfs = []

    for sheet, area, momento in sheets:
        df_out = cargar_y_procesar(xls, sheet, area, momento)
        if df_out is not None:
            dfs.append(df_out)

    if not dfs:
        raise ValueError("No se generaron dataframes procesados. Revisar hojas de entrada.")

    bd_final = pd.concat(dfs, ignore_index=True)

    bd_final["EVALUACION__c"] = np.where(
        (bd_final["ANIO"] == 2025) & (bd_final["MOMENTO"] == "LB"),
        "2025-I",
        "2025-II"
    )

    bd_final = bd_final.merge(
        df_equivalencias,
        on=["AREA_EVALUACION", "COMPETENCIA"],
        how="left"
    )

    bd_final = bd_final.loc[:, ~bd_final.columns.str.contains("^Unnamed")]

    bd_final = bd_final.rename(columns={
        "DNI": "DNI__c",
        "COMPETENCIA": "Competencia__c",
        "COMPETENCIA_TEXTO": "Competencia_texto__c",
        "VALOR_PROMEDIO": "Valor_Promedio__c",
        "VALOR_LOGRO": "Valor_alcanzado__c",
        "APELLIDOS Y NOMBRES": "APELLIDOS_Y_NOMBRES__c",
        "GRADO": "GRADO__c",
        "SEXO": "SEXO__c",
        "CENTRO": "CENTRO__c",
        "CONDICIÓN ACTUAL": "CONDICIN_ACTUAL__c",
        "AREA_EVALUACION": "REA__c",
        "TOTAL PREGUNTAS ACERTADAS": "TOTAL_PREGUNTAS_ACERTADAS__c",
        "PORCENTAJE_PROMEDIO": "PORCENTAJE_DE_PROMEDIO__c",
        "NIVEL_LOGRO_GENERAL": "NIVEL_DE_LOGRO__c",
        "MOMENTO": "ESCENARIO__c",
        "ANIO": "ANIO__c"
    })

    bd_final = bd_final.drop(columns=["AREA_EVALUACION"], errors="ignore")

    bd_final["Valor_Promedio__c"] = pd.to_numeric(
        bd_final["Valor_Promedio__c"], errors="coerce"
    )

    bd_final["TOTAL_PREGUNTAS_ACERTADAS__c"] = pd.to_numeric(
        bd_final["TOTAL_PREGUNTAS_ACERTADAS__c"], errors="coerce"
    )

    bd_final["PORCENTAJE_DE_PROMEDIO__c"] = (
        bd_final["PORCENTAJE_DE_PROMEDIO__c"]
        .astype(str)
        .str.replace("%", "", regex=False)
    )

    bd_final["PORCENTAJE_DE_PROMEDIO__c"] = pd.to_numeric(
        bd_final["PORCENTAJE_DE_PROMEDIO__c"], errors="coerce"
    )

    bd_final["ANIO__c"] = pd.to_numeric(
        bd_final["ANIO__c"], errors="coerce"
    )

    return bd_final


def main() -> None:
    print("=== INICIANDO TRANSFORMACIÓN ===")

    bd_final = transformar_archivo(INPUT_FILE)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    bd_final.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL
    )

    print("✅ TRANSFORMACIÓN COMPLETADA")
    print(f"Archivo generado: {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()
