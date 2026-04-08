from pathlib import Path
import unicodedata

import pandas as pd


INPUT_FILE = Path("data/raw/2025/progreso_curricular/curricula_after_school_2025.xlsx")
OUTPUT_DIR = Path("data/processed/2025/progreso_curricular")
OUTPUT_FILE = OUTPUT_DIR / "BD_Curricula_Consolidada.csv"

HOJAS_VALIDAS = [
    "01 Pachacutec",
    "02 HDMP",
    "03 Santa Rosa",
    "04 Pte Piedra",
    "05 Cuzco",
    "06 Huancavelica - IE 36003",
    "07 Huarochiri",
    "08 VMT Paraíso",
    "09 Huancavelica - IE 36002",
]


def normalizar_texto(x: str) -> str:
    return (
        unicodedata.normalize("NFKD", str(x))
        .encode("ascii", "ignore")
        .decode("utf-8")
        .strip()
        .upper()
    )


def cargar_archivo() -> dict[str, pd.DataFrame]:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {INPUT_FILE}")

    sheets_all = pd.read_excel(INPUT_FILE, sheet_name=None)
    sheets_dict = {k: v for k, v in sheets_all.items() if k in HOJAS_VALIDAS}

    faltantes = [h for h in HOJAS_VALIDAS if h not in sheets_dict]
    if faltantes:
        print("⚠️ Hojas no encontradas (revisar nombres exactos):", faltantes)

    return sheets_dict


def transformar() -> pd.DataFrame:
    sheets_dict = cargar_archivo()

    dfs = []

    for hoja, df in sheets_dict.items():
        df = df.copy()

        df.columns = [normalizar_texto(c) for c in df.columns]
        df = df.drop(columns=["CAPACIDAD"], errors="ignore")
        df["SEDE"] = normalizar_texto(hoja)

        dfs.append(df)

    if not dfs:
        raise ValueError("No se encontraron hojas válidas para consolidar.")

    bd_curricula_consolidada = pd.concat(dfs, ignore_index=True)

    return bd_curricula_consolidada


def main() -> None:
    print("=== TRANSFORMACIÓN PROGRESO CURRICULAR ===")

    df_final = transformar()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("✅ Consolidado creado:", df_final.shape)
    print("Columnas finales:", list(df_final.columns))
    print("Archivo generado:", OUTPUT_FILE.resolve())


if __name__ == "__main__":
    main()
