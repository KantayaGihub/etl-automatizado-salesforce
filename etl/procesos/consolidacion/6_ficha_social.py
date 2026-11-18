from pathlib import Path
import pandas as pd

# ============================================================
# CONFIG
# ============================================================
ARCHIVO_FUENTE = Path("data/processed/2026/6.ficha_social/Ficha_Social_v2.xlsx")

CARPETA_SALIDA = Path("data/consolidated/ficha_social")
ARCHIVO_SALIDA = CARPETA_SALIDA / "Ficha_Social_v2.xlsx"

def leer_excel_seguro(ruta: Path) -> pd.DataFrame:
    if not ruta.exists():
        print(f"No se encontró la fuente compartida: {ruta}")
        return pd.DataFrame()

    print(f"Leyendo fuente compartida: {ruta}")
    df = pd.read_excel(ruta, sheet_name="Ficha_Social_v2", dtype=str)
    print(f"-> {len(df)} filas")
    return df


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

    consolidado = leer_excel_seguro(ARCHIVO_FUENTE)
    if consolidado.empty:
        raise ValueError("La fuente compartida no contiene registros para preparar.")

    print(f"Total preparado desde la fuente compartida: {len(consolidado)} filas")

    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)

    df_calidad = codebook(consolidado)

    with pd.ExcelWriter(ARCHIVO_SALIDA, engine="openpyxl") as writer:
        consolidado.to_excel(writer, index=False, sheet_name="Ficha_Social_v2")
        df_calidad.to_excel(writer, index=False, sheet_name="Codebook")

    print(f"Consolidado guardado en: {ARCHIVO_SALIDA}")
    print(f"Total final: {len(consolidado)} filas")


if __name__ == "__main__":
    main()
