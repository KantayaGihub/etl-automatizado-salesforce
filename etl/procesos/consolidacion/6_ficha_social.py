from pathlib import Path
import pandas as pd

# ============================================================
# CONFIG
# ============================================================
ARCHIVOS_POR_ANIO = {
    "2025": Path("data/processed/2025/6.ficha_social/Ficha_Social_v2.xlsx"),
    "2026": Path("data/processed/2026/6.ficha_social/Ficha_Social_v2.xlsx"),
}

CARPETA_SALIDA = Path("data/consolidated/ficha_social")
ARCHIVO_SALIDA = CARPETA_SALIDA / "Ficha_Social_v2.xlsx"


def leer_excel_seguro(ruta: Path, anio: str) -> pd.DataFrame:
    if not ruta.exists():
        print(f"⚠️ No se encontró archivo para {anio}: {ruta}")
        return pd.DataFrame()

    print(f"📂 Leyendo {anio}: {ruta}")
    df = pd.read_excel(ruta, sheet_name="Ficha_Social_v2", dtype=str)
    df["ANIO_FUENTE"] = anio
    print(f"   -> {len(df)} filas")
    return df


def homologar_columnas(dataframes):
    todas_las_columnas = set()

    for df in dataframes:
        todas_las_columnas.update(df.columns)

    todas_las_columnas = list(todas_las_columnas)

    dfs_homologados = []
    for df in dataframes:
        for col in todas_las_columnas:
            if col not in df.columns:
                df[col] = None
        df = df[todas_las_columnas]
        dfs_homologados.append(df)

    return dfs_homologados


def codebook(df):
    resumen = pd.DataFrame({
        "Tipo": df.dtypes,
        "Nulos (#)": df.isnull().sum(),
        "Porcentaje Nulos (%)": df.isnull().mean() * 100,
        "Valores únicos (#)": df.nunique(),
    })

    resumen["Valores únicos (Muestra)"] = df.apply(
        lambda x: str(list(x.dropna().unique()[:5]))
    )

    return resumen.reset_index().rename(columns={"index": "Variable"})


def main():
    print("=== CONSOLIDACIÓN FICHA SOCIAL ===")

    dfs = []
    for anio, ruta in ARCHIVOS_POR_ANIO.items():
        df = leer_excel_seguro(ruta, anio)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        raise FileNotFoundError("No se encontró ninguna base procesada para consolidar.")

    dfs = homologar_columnas(dfs)

    consolidado = pd.concat(dfs, ignore_index=True)

    print(f"📊 Total concatenado: {len(consolidado)} filas")

    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)

    df_calidad = codebook(consolidado)

    with pd.ExcelWriter(ARCHIVO_SALIDA, engine="openpyxl") as writer:
        consolidado.to_excel(writer, index=False, sheet_name="Ficha_Social_v2")
        df_calidad.to_excel(writer, index=False, sheet_name="Codebook")

    print(f"✅ Consolidado guardado en: {ARCHIVO_SALIDA}")
    print(f"📦 Total final: {len(consolidado)} filas")


if __name__ == "__main__":
    main()
