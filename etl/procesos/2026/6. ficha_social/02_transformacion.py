import pandas as pd
import numpy as np
from pathlib import Path
import sys

# ============================================================
# 0. RENOMBRAR COLUMNAS DUPLICADAS
# ============================================================
def renombrar_columnas_duplicadas(df):

    cols = df.columns
    nuevos = []
    contador = {}

    for c in cols:

        if c not in contador:

            contador[c] = 0
            nuevos.append(c)

        else:

            contador[c] += 1
            nuevos.append(f"{c}_v{contador[c]}")

    df = df.copy()
    df.columns = nuevos

    return df


# ============================================================
# 1. CODEBOOK SIMPLE
# ============================================================
def codebook(df, pk_col):

    print("Generando métricas de calidad...")

    resumen = pd.DataFrame({

        "Tipo": df.dtypes,

        "Nulos (#)": df.isnull().sum(),

        "Porcentaje Nulos (%)": (
            df.isnull().mean() * 100
        ).round(2),

        "Valores únicos (#)": df.nunique(),

    })

    print("Calculando mínimos y máximos...")

    resumen["Mínimo"] = df.apply(

        lambda x:

        x.min(skipna=True)

        if pd.api.types.is_numeric_dtype(x)

        else None

    )

    resumen["Máximo"] = df.apply(

        lambda x:

        x.max(skipna=True)

        if pd.api.types.is_numeric_dtype(x)

        else None

    )

    print("Verificando duplicados...")

    resumen["Duplicados (Valores)"] = "No"

    if pk_col in df.columns:

        total_dups = (

            df[pk_col]
            .dropna()
            .duplicated()
            .sum()

        )

        if total_dups > 0:

            resumen.loc[
                pk_col,
                "Duplicados (Valores)"
            ] = f"Sí ({total_dups} duplicados)"

            print(
                f"Duplicados encontrados "
                f"en PK ({pk_col}): {total_dups}"
            )

        else:

            resumen.loc[
                pk_col,
                "Duplicados (Valores)"
            ] = "No (PK válida)"

            print(
                f"PK válida "
                f"({pk_col})"
            )

    for col in df.columns:

        if col != pk_col:

            if df[col].duplicated().any():

                resumen.loc[
                    col,
                    "Duplicados (Valores)"
                ] = "Sí"

    print("Generando muestra de valores únicos...")

    resumen["Valores únicos (Muestra)"] = df.apply(

        lambda x:

        str(list(x.dropna().unique()[:5]))

    )

    resumen = (

        resumen
        .reset_index()
        .rename(columns={
            "index": "Variable"
        })

    )

    return resumen


# ============================================================
# 2. FUNCIÓN PRINCIPAL
# ============================================================
def limpiar_y_generar_ficha_social_v2(
    file_path,
    output_folder
):

    print(f"--- Procesando Ficha Social (v2) ---")

    print(
        f"Archivo de entrada esperado: "
        f"{file_path}"
    )

    # ========================================================
    # VALIDAR EXISTENCIA
    # ========================================================
    if not Path(file_path).exists():

        print(
            f"ERROR: "
            f"No se encontró el archivo "
            f"{file_path}"
        )

        sys.exit(1)

    # ========================================================
    # CARGAR EXCEL
    # ========================================================
    df = pd.read_excel(

        file_path,

        dtype={

            "Número de Documento": str,

            "Número de Documento.1": str,

            "Número de documento del niño": str

        }

    )

    print("Archivo cargado correctamente")

    # ========================================================
    # RENOMBRAR DUPLICADAS
    # ========================================================
    df = renombrar_columnas_duplicadas(df)

    # ========================================================
    # CONVERTIR FECHAS
    # ========================================================
    if "Marca temporal" in df.columns:

        df["Marca temporal"] = pd.to_datetime(

            df["Marca temporal"],

            errors="ignore"

        )

    # ========================================================
    # LIMPIEZA DNI
    # ========================================================
    col_dni = "Número de documento del niño"

    df[col_dni] = (

        df[col_dni]

        .astype(str)

        .str.replace(
            r"\.0$",
            "",
            regex=True
        )

    )

    # ========================================================
    # DEDUPLICACIÓN
    # ========================================================
    df = df.sort_values(
        "Marca temporal",
        ascending=True
    )

    df = df.drop_duplicates(
        subset=[col_dni],
        keep="last"
    )

    # ========================================================
    # ASEGURAR STRINGS
    # ========================================================
    for col in [

        "Número de Documento",

        "Número de Documento.1",

        col_dni

    ]:

        if col in df.columns:

            df[col] = df[col].astype(str)

    # ========================================================
    # CREAR CARPETA SALIDA
    # ========================================================
    output_folder = Path(output_folder)

    output_folder.mkdir(
        parents=True,
        exist_ok=True
    )

    # ========================================================
    # ARCHIVOS SALIDA
    # ========================================================
    out_path = (
        output_folder /
        "Ficha_Social_v2.xlsx"
    )

    calidad_path = (
        output_folder /
        "Ficha_Social_v2_CALIDAD.xlsx"
    )

    # ========================================================
    # GENERAR CODEBOOK
    # ========================================================
    print("\n GENERANDO REPORTE DE CALIDAD...")

    df_code = codebook(
        df,
        pk_col=col_dni
    )

    # ========================================================
    # GUARDAR EXCEL FINAL
    # ========================================================
    with pd.ExcelWriter(
        out_path,
        engine="openpyxl"
    ) as writer:

        df.to_excel(

            writer,

            index=False,

            sheet_name="Ficha_Social_v2"

        )

    # ========================================================
    # GUARDAR CODEBOOK
    # ========================================================
    df_code.to_excel(

        calidad_path,

        index=False,

        engine="openpyxl"

    )

    print(
        f"Archivo final generado "
        f"→ {out_path}"
    )

    print(
        f"Reporte calidad generado "
        f"→ {calidad_path}"
    )


# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================
if __name__ == "__main__":

    limpiar_y_generar_ficha_social_v2(

        file_path=(
            "data/raw/2026/6.ficha_social/"
            "Ficha_Social.xlsx"
        ),

        output_folder=(
            "data/processed/2026/6.ficha_social"
        )

    )
