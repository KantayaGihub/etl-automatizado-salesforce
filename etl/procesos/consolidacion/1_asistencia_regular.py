from pathlib import Path
import pandas as pd

# ============================================================
# CONFIG
# ============================================================
ARCHIVOS_POR_ANIO = {
    "2025": Path("data/processed/2025/asistencia_regular/asistencias_consolidado_kantaya.csv"),
    "2026": Path("data/processed/2026/asistencia_regular/asistencias_consolidado_kantaya.csv"),
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


def main():
    print("=== CONSOLIDACIÓN ASISTENCIA REGULAR ===")

    dfs = []
    for anio, ruta in ARCHIVOS_POR_ANIO.items():
        df = leer_csv_seguro(ruta, anio)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        raise FileNotFoundError("No se encontró ninguna base procesada para consolidar.")

    dfs = homologar_columnas(dfs)

    consolidado = pd.concat(dfs, ignore_index=True)

    print(f"📊 Total concatenado: {len(consolidado)} filas")

    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)
    consolidado.to_csv(ARCHIVO_SALIDA, index=False, encoding="utf-8-sig")

    print(f"✅ Consolidado guardado en: {ARCHIVO_SALIDA}")
    print(f"📦 Total final: {len(consolidado)} filas")


if __name__ == "__main__":
    main()
