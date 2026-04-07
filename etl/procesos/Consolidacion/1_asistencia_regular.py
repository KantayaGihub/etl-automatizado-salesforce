from pathlib import Path
import pandas as pd

# ============================================================
# CONFIG
# ============================================================
BASE_PROCESSED = Path("data/processed")
ANIOS = ["2025", "2026"]

ARCHIVOS_POR_ANIO = {
    "2025": BASE_PROCESSED / "2025" / "asistencia_regular" / "asistencias_2025_limpio.csv",
    "2026": BASE_PROCESSED / "2026" / "asistencia_regular" / "asistencias_2026_limpio.csv",
}

CARPETA_SALIDA = Path("data/consolidated/asistencia_regular")
ARCHIVO_SALIDA = CARPETA_SALIDA / "asistencias_consolidado_kantaya.csv"


def leer_csv_seguro(ruta: Path, anio: str) -> pd.DataFrame:
    if not ruta.exists():
        print(f"⚠️ No se encontró archivo para {anio}: {ruta}")
        return pd.DataFrame()

    print(f"📂 Leyendo {anio}: {ruta}")
    df = pd.read_csv(ruta, dtype={"DNI": str})
    df["ANIO_FUENTE"] = anio
    print(f"   -> {len(df)} filas")
    return df


def homologar_columnas(dataframes: list[pd.DataFrame]) -> list[pd.DataFrame]:
    """
    Une todas las columnas posibles entre años.
    Si una base no tiene alguna columna, la crea en blanco.
    """
    todas_las_columnas = set()
    for df in dataframes:
        todas_las_columnas.update(df.columns)

    todas_las_columnas = sorted(todas_las_columnas)

    dfs_homologados = []
    for df in dataframes:
        for col in todas_las_columnas:
            if col not in df.columns:
                df[col] = None
        df = df[todas_las_columnas]
        dfs_homologados.append(df)

    return dfs_homologados


def quitar_duplicados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta la llave según tu negocio.
    Esta es una propuesta común para asistencias.
    """
    columnas_clave = [c for c in ["DNI", "FECHA", "CURSO", "SEDE"] if c in df.columns]

    if columnas_clave:
        antes = len(df)
        df = df.drop_duplicates(subset=columnas_clave, keep="last")
        despues = len(df)
        print(f"🧹 Duplicados removidos: {antes - despues}")
    else:
        antes = len(df)
        df = df.drop_duplicates()
        despues = len(df)
        print(f"🧹 Duplicados exactos removidos: {antes - despues}")

    return df


def main():
    print("=== CONSOLIDACIÓN ASISTENCIA REGULAR ===")

    dfs = []
    for anio in ANIOS:
        ruta = ARCHIVOS_POR_ANIO[anio]
        df = leer_csv_seguro(ruta, anio)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        raise FileNotFoundError("No se encontró ninguna base procesada para consolidar.")

    dfs = homologar_columnas(dfs)

    consolidado = pd.concat(dfs, ignore_index=True)
    print(f"📊 Total concatenado: {len(consolidado)} filas")

    consolidado = quitar_duplicados(consolidado)

    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)
    consolidado.to_csv(ARCHIVO_SALIDA, index=False, encoding="utf-8-sig")

    print(f"✅ Consolidado guardado en: {ARCHIVO_SALIDA}")
    print(f"📦 Total final: {len(consolidado)} filas")


if __name__ == "__main__":
    main()
