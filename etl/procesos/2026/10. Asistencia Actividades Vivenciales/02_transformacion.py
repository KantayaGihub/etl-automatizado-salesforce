from pathlib import Path
import pandas as pd


INPUT_FILE = Path("data/raw/2026/10.asistencia_actividades_vivenciales/asistencia_actividades_vivenciales_2026.xlsx")
OUTPUT_DIR = Path("data/processed/2026/10.asistencia_actividades_vivenciales")
OUTPUT_FILE = OUTPUT_DIR / "BD_asistencia_actividades_vivenciales.csv"

# ============================================================
# NUEVO: ARCHIVO CALIDAD
# ============================================================
QUALITY_FILE = (
    OUTPUT_DIR /
    "BD_asistencia_actividades_vivenciales_CALIDAD.xlsx"
)

SHEET_NAME = "H4 Registro niños"


# ============================================================
# NUEVO: CODEBOOK
# ============================================================
def codebook(df, pk_col=None):

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

    if pk_col and pk_col in df.columns:

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


def cargar_archivo():

    if not INPUT_FILE.exists():

        raise FileNotFoundError(
            f"No existe el archivo: {INPUT_FILE}"
        )

    return pd.read_excel(
        INPUT_FILE,
        sheet_name=SHEET_NAME
    )


def transformar():

    BD = cargar_archivo()

    BD2 = BD.copy()

    # Filtrar filas con DNI
    BD2 = (
        BD2[
            BD2.iloc[:, 1].notna()
        ]
        .reset_index(drop=True)
    )

    # Extraer encabezados dinámicos
    actividad_row = BD.iloc[1]

    fecha_row = pd.to_datetime(
        BD.iloc[2],
        errors="coerce"
    )

    fecha_str = fecha_row.dt.strftime("%Y-%m-%d")

    new_columns = []

    for col in range(BD.shape[1]):

        actividad = (
            str(actividad_row[col])

            if pd.notna(
                actividad_row[col]
            )

            else ""
        )

        fecha = (
            str(fecha_str[col])

            if pd.notna(
                fecha_str[col]
            )

            else ""
        )

        nombre = " - ".join([

            x for x in [
                fecha,
                actividad
            ]

            if x

        ]).strip(" -")

        new_columns.append(nombre)

    BD2.columns = new_columns

    # Quitar filas header
    BD2 = (
        BD2.iloc[2:]
        .reset_index(drop=True)
    )

    # Quitar columna índice
    BD2 = BD2.iloc[:, 1:]

    # Renombrar columnas
    BD2.columns.values[0] = "DNI"
    BD2.columns.values[1] = "NOMBRES Y APELLIDOS"
    BD2.columns.values[2] = "GRADO"
    BD2.columns.values[3] = "SEXO"
    BD2.columns.values[4] = "CENTRO"
    BD2.columns.values[5] = "ESTADO"

    BD2.columns.values[-4] = "ASISTENCIAS PROGRAMADAS"
    BD2.columns.values[-3] = "ASISTENCIAS REALES"
    BD2.columns.values[-2] = "% PART."
    BD2.columns.values[-1] = "CONDICIÓN DE PARTICIPACIÓN"

    id_vars = [

        "DNI",
        "NOMBRES Y APELLIDOS",
        "GRADO",
        "SEXO",
        "CENTRO",
        "ESTADO",
        "ASISTENCIAS PROGRAMADAS",
        "ASISTENCIAS REALES",
        "% PART.",
        "CONDICIÓN DE PARTICIPACIÓN",

    ]

    activity_cols = [

        c for c in BD2.columns

        if str(c).startswith("2026-")

    ]

    BD_long = BD2.melt(

        id_vars=id_vars,

        value_vars=activity_cols,

        var_name="FECHA_ACTIVIDAD",

        value_name="VALOR"

    )

    split_cols = (

        BD_long["FECHA_ACTIVIDAD"]

        .str.split(
            " - ",
            n=1,
            expand=True
        )

    )

    if split_cols.shape[1] == 1:
        split_cols[1] = None

    BD_long["FECHA"] = pd.to_datetime(
        split_cols[0],
        errors="coerce"
    )

    BD_long["ACTIVIDAD"] = split_cols[1]

    BD_long["MES"] = (
        BD_long["FECHA"]
        .dt.month_name()
    )

    BD_long = (

        BD_long
        .drop(columns=["FECHA_ACTIVIDAD"])
        .reset_index(drop=True)

    )

    return BD_long


def main():

    print(
        "=== TRANSFORMACIÓN "
        "ASISTENCIA ACTIVIDADES "
        "VIVENCIALES ==="
    )

    df = transformar()

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    # ========================================================
    # EXPORTAR CSV
    # ========================================================
    df.to_csv(

        OUTPUT_FILE,

        index=False,

        encoding="utf-8-sig"

    )

    print("Archivo generado")

    print(df.shape)

    print(OUTPUT_FILE.resolve())

    # ========================================================
    # NUEVO: GENERAR CODEBOOK
    # ========================================================
    print("\n GENERANDO REPORTE DE CALIDAD...")

    df_code = codebook(
        df,
        pk_col="DNI"
    )

    df_code.to_excel(

        QUALITY_FILE,

        index=False,

        engine="openpyxl"

    )

    print("Reporte calidad generado")

    print(QUALITY_FILE.resolve())


if __name__ == "__main__":

    main()
