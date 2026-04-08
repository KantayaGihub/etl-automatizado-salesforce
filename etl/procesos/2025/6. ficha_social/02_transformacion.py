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

# ============================================================
# 2. FUNCIÓN PRINCIPAL
# ============================================================
def limpiar_y_generar_ficha_social_v2(file_path, output_folder):

    print(f"--- Procesando Ficha Social (v2) ---")
    print(f"🔎 Archivo de entrada esperado: {file_path}")

    # Validar existencia de archivo
    if not Path(file_path).exists():
        print(f"❌ ERROR: No se encontró el archivo {file_path}")
        sys.exit(1)

    # Cargar con tipos correctos
    df = pd.read_excel(
        file_path,
        dtype={
            "Número de Documento": str,
            "Número de Documento.1": str,
            "Número de documento del niño": str
        }
    )

    print("✔ Archivo cargado correctamente")

    # Renombrar duplicadas
    df = renombrar_columnas_duplicadas(df)

    # Convertir marca temporal
    if "Marca temporal" in df.columns:
        df["Marca temporal"] = pd.to_datetime(df["Marca temporal"], errors="ignore")

    # Limpieza del DNI
    col_dni = "Número de documento del niño"
    df[col_dni] = df[col_dni].astype(str).str.replace(r"\.0$", "", regex=True)

    # Deduplicación
    df = df.sort_values("Marca temporal", ascending=True)
    df = df.drop_duplicates(subset=[col_dni], keep="last")

    # Asegurar strings
    for col in ["Número de Documento", "Número de Documento.1", col_dni]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Crear carpeta salida
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    out_path = output_folder / "Ficha_Social_v2.xlsx"
    df_code = codebook(df, pk_col=col_dni)

    # Guardar Excel final
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Ficha_Social_v2")
        df_code.to_excel(writer, index=False, sheet_name="Codebook")

    print(f"🎉 Archivo final generado con éxito → {out_path}")


# ============================================================
# EJECUCIÓN DIRECTA DESDE TERMINAL / GITHUB ACTIONS
# ============================================================
if __name__ == "__main__":
    limpiar_y_generar_ficha_social_v2(
        file_path="data/raw/2025/6.ficha_social/Ficha_Social.xlsx",
        output_folder="data/processed/2025/6.ficha_social"
    )
