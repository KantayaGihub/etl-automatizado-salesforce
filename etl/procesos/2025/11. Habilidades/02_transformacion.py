from pathlib import Path
import pandas as pd
import unicodedata


INPUT_FILE = Path("data/raw/2025/impacto_habilidades/pe25_ventanilla_proyectos_habilidades.xlsx")
OUTPUT_DIR = Path("data/processed/2025/impacto_habilidades")
OUTPUT_FILE = OUTPUT_DIR / "BD_impacto_habilidades.csv"

SHEETS_MAP = {
    "H_Autogestión": "Autogestión",
    "H_Sociales": "Sociales",
    "H_Investigación": "Investigación",
}


def normalizar_texto(texto: str) -> str:
    return (
        unicodedata.normalize("NFKD", str(texto))
        .encode("ascii", "ignore")
        .decode("utf-8")
        .strip()
    )


def nivel_orden(valor: str):
    if pd.isna(valor):
        return None

    valor_norm = normalizar_texto(valor).lower()

    mapping = {
        "en inicio": 1,
        "en proceso": 2,
        "logrado": 3,
        "sobresaliente": 4,
    }
    return mapping.get(valor_norm)


def transformar_hoja(df: pd.DataFrame, habilidad_nombre: str) -> pd.DataFrame:
    df = df.copy()

    # limpiar nombres de columnas
    df.columns = [normalizar_texto(c) for c in df.columns]

    rename_map = {
        "N°": "Nro",
        "DNI": "DNI__c",
        "Apellidos y nombres": "Apellidos_y_nombres__c",
        "Grado": "Grado__c",
        "Sexo": "Sexo__c",
        "Centro": "Centro__c",
        "Permanencia": "Permanencia__c",
        "Condicion actual": "Condicin_actual__c",
        "Condición actual": "Condicin_actual__c",
        "Habilidades": "Habilidades__c",
        "Nivel de logro Base": "Nivel_de_logro_Base",
        "Nivel de logro Salida": "Nivel_de_logro_Salida",
    }

    df = df.rename(columns=rename_map)

    # si por alguna razón la columna Habilidades viene vacía o inconsistente,
    # usamos el nombre de la hoja como respaldo
    if "Habilidades__c" not in df.columns:
        df["Habilidades__c"] = habilidad_nombre
    else:
        df["Habilidades__c"] = df["Habilidades__c"].fillna(habilidad_nombre)

    columnas_base = [
        "DNI__c",
        "Apellidos_y_nombres__c",
        "Grado__c",
        "Sexo__c",
        "Centro__c",
        "Permanencia__c",
        "Condicin_actual__c",
        "Habilidades__c",
    ]

    columnas_presentes = [c for c in columnas_base if c in df.columns]

    # pasar LB y LS a largo
    df_long = df.melt(
        id_vars=columnas_presentes,
        value_vars=["Nivel_de_logro_Base", "Nivel_de_logro_Salida"],
        var_name="tipo_logro",
        value_name="Nivel_de_logro__c",
    )

    df_long["Escenario"] = df_long["tipo_logro"].map({
        "Nivel_de_logro_Base": "LB",
        "Nivel_de_logro_Salida": "LS",
    })

    df_long["Nivel_logroOrden"] = df_long["Nivel_de_logro__c"].apply(nivel_orden)

    df_long = df_long.drop(columns=["tipo_logro"])

    # limpiar filas sin DNI o sin nivel
    df_long = df_long[df_long["DNI__c"].notna()].copy()
    df_long = df_long[df_long["Nivel_de_logro__c"].notna()].copy()

    # evaluación dinámica
    df_long["Evaluacion__c"] = df_long["Escenario"].map({
        "LB": "2025-I",
        "LS": "2025-II",
    })

    return df_long


def transformar_archivo() -> pd.DataFrame:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"No existe el archivo: {INPUT_FILE}")

    dfs = []

    for sheet_name, habilidad_nombre in SHEETS_MAP.items():
        df_sheet = pd.read_excel(INPUT_FILE, sheet_name=sheet_name)
        df_transformado = transformar_hoja(df_sheet, habilidad_nombre)
        dfs.append(df_transformado)

    df_final = pd.concat(dfs, ignore_index=True)

    return df_final


def main():
    df_final = transformar_archivo()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("✅ Archivo generado")
    print(df_final.shape)
    print(OUTPUT_FILE.resolve())


if __name__ == "__main__":
    main()
