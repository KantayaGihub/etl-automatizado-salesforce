#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os
import glob
import re
from datetime import datetime
import unicodedata

# ============================================================
# MÓDULO DE CALIDAD
# ============================================================
def codebook(df, pk_col):
    print("   [Calidad] Calculando tipos, nulos y únicos...")
    resumen = pd.DataFrame({
        "Tipo": df.dtypes,
        "Nulos (#)": df.isnull().sum(),
        "Porcentaje Nulos (%)": ((df.isnull().sum() / len(df)) * 100),
        "Valores únicos (#)": df.nunique(),
    })

    print("   [Calidad] Calculando min/max numéricos...")
    resumen["Mínimo"] = df.apply(lambda x: x.min(skipna=True) if pd.api.types.is_numeric_dtype(x) else None)
    resumen["Máximo"] = df.apply(lambda x: x.max(skipna=True) if pd.api.types.is_numeric_dtype(x) else None)

    print(f"   [Calidad] Verificando duplicados (PK={pk_col})...")
    resumen["Duplicados (Valores)"] = "No"

    if pk_col in df.columns:
        total_dups = df[pk_col].dropna().duplicated().sum()
        if total_dups > 0:
            print(f"   -> ALERTA PK ({pk_col}): {total_dups} duplicados encontrados.")
            resumen.loc[pk_col, "Duplicados (Valores)"] = f"¡SÍ! ({total_dups})"
        else:
            resumen.loc[pk_col, "Duplicados (Valores)"] = "No (PK válida)"

    resumen = resumen.reset_index().rename(columns={"index": "Variable"})
    return resumen


# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================
def limpiar_y_deduplicar_encuesta_padre(input_folder, output_csv, output_excel):

    print(f"--- Procesando carpeta: {input_folder} ---")

    archivos = glob.glob(os.path.join(input_folder, "*.csv"))

    if not archivos:
        raise FileNotFoundError(f"No se encontraron CSV en {input_folder}")

    dfs = []
    for f in archivos:
        print(f"→ Cargando {os.path.basename(f)}")
        dfs.append(pd.read_csv(f))

    df = pd.concat(dfs, ignore_index=True)
    print(f"Total filas combinadas: {len(df)}")

    # Renombrar columnas
    df = df.rename(columns={
        'Documento de identidad': 'Documento de identidad Apoderado',
        'Documento de identidad ': 'Documento de identidad Niño',
        'N° de documento': 'N Documento Apoderado',
        'N° de documento ': 'N Documento Niño'
    })

    col_pk = 'N Documento Apoderado'

    # Limpiar DNI
    if col_pk in df.columns:
        df[col_pk] = (
            df[col_pk].astype(str)
            .str.replace(r'\.0$', '', regex=True)
            .str.replace(r'\D', '', regex=True)
        )
        df[col_pk] = df[col_pk].replace(['nan', '', '0'], np.nan)

    # Fecha
    if 'Marca temporal' in df.columns:
        df['Marca temporal'] = pd.to_datetime(df['Marca temporal'], errors='coerce', dayfirst=True)

    # Limpieza texto
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('ascii')
            .str.replace(r"\s+", " ", regex=True)
            .str.upper()
            .str.strip()
            .replace('NAN', np.nan)
            .replace('', np.nan)
        )

    # Deduplicación
    df = df.sort_values(by='Marca temporal', ascending=True)
    df = df.drop_duplicates(subset=[col_pk], keep='last')

    if 'Marca temporal' in df.columns:
        df['Marca temporal'] = df['Marca temporal'].dt.date

    print(f"Filas finales: {len(df)}")

    # Guardar
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"Archivo guardado: {output_csv}")

    df_calidad = codebook(df, col_pk)
    df_calidad.to_excel(output_excel, index=False)
    print(f"Reporte de calidad: {output_excel}")


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":

    # Entrada RAW
    input_excel = "data/raw/2025/5.encuesta_satisfaccion_padres"

    # Carpeta temporal CSV
    input_csv = "data/raw/2025/5.encuesta_satisfaccion_padres/csv"
    os.makedirs(input_csv, exist_ok=True)

    print("\n=== Convirtiendo Excel a CSV ===")

    VALID_FILES = {
        "Encuesta_Inicial_1y2.xlsx",
        "Encuesta_3y4.xlsx",
        "Encuesta_5y6.xlsx"
    }

    for fname in os.listdir(input_excel):
        if fname in VALID_FILES:
            excel_path = os.path.join(input_excel, fname)
            csv_path = os.path.join(input_csv, fname.replace(".xlsx", ".csv"))

            print(f"→ {fname}")
            df_temp = pd.read_excel(excel_path)
            df_temp.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print("✓ Conversión completa\n")

    # Salida PROCESSED
    output_dir = "data/processed/2025/5.encuesta_satisfaccion_padres"
    os.makedirs(output_dir, exist_ok=True)

    output_csv = os.path.join(output_dir, "Encuesta_Padres_Deduplicado.csv")
    output_excel = os.path.join(output_dir, "Encuesta_Padres_Deduplicado_CALIDAD.xlsx")

    limpiar_y_deduplicar_encuesta_padre(
        input_csv,
        output_csv,
        output_excel
    )
