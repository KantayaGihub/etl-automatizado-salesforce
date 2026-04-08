from pathlib import Path
import pandas as pd
import numpy as np
import unicodedata


# ==========================================================
# PATHS
# ==========================================================

INPUT_FILE = Path("data/raw/2025/horario_regular/horario_regular_2025.xlsx")
OUTPUT_DIR = Path("data/processed/2025/horario_regular")
OUTPUT_FILE = OUTPUT_DIR / "base_horaria_final.csv"

SEMANAS_POR_BIMESTRE = 8
ANIO = 2025


# ==========================================================
# UTILIDADES
# ==========================================================

def quitar_tildes(texto):
    return (
        unicodedata.normalize("NFKD", str(texto))
        .encode("ascii", "ignore")
        .decode("utf-8")
    )

def normalizar(texto):
    return quitar_tildes(str(texto)).strip().upper()


# ==========================================================
# CARGA
# ==========================================================

def cargar_excel():
    df = pd.read_excel(INPUT_FILE, sheet_name="Distribución horaria")

    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    df = df.drop(index=[0,1,2,3], errors="ignore")
    df = df.drop(index=range(60,72), errors="ignore")
    df = df.drop(columns=["Unnamed: 6"], errors="ignore")

    df = df.iloc[:, :5].copy()
    df.columns = ["concepto","CICLO_II","CICLO_III","CICLO_IV","CICLO_V"]

    return df


# ==========================================================
# PROCESAMIENTO
# ==========================================================

def construir_base():

    df = cargar_excel()

    sedes_base = [
        "VENTANILLA",
        "PUENTE PIEDRA",
        "CUSCO",
        "HUANCAVELICA",
        "HUAROCHIRÍ",
        "VMT - PARAISO"
    ]

    df["sede"] = df["concepto"].where(df["concepto"].isin(sedes_base)).bfill()

    fila_var = df[df["concepto"]=="VARIABLES"].iloc[0]

    map_grado = {
        "CICLO_II": fila_var["CICLO_II"],
        "CICLO_III": fila_var["CICLO_III"],
        "CICLO_IV": fila_var["CICLO_IV"],
        "CICLO_V": fila_var["CICLO_V"],
    }

    df = df[df["concepto"].str.startswith("N° HORAS SEMANALES", na=False)].copy()

    df["area"] = df["concepto"].str.replace(
        "N° HORAS SEMANALES |","",regex=False
    ).str.strip()

    df = df.melt(
        id_vars=["sede","area"],
        value_vars=["CICLO_II","CICLO_III","CICLO_IV","CICLO_V"],
        var_name="ciclo",
        value_name="horas_semanales"
    )

    df["grado"] = df["ciclo"].map(map_grado)
    df["horas_semanales"] = pd.to_numeric(df["horas_semanales"], errors="coerce")

    df = df.dropna(subset=["sede","grado","horas_semanales"])

    expansion = {
        "INICIAL":["INICIAL"],
        "1ER Y 2DO GRADO":["1ER GRADO","2DO GRADO"],
        "3ER Y 4TO GRADO":["3ER GRADO","4TO GRADO"],
        "5TO Y 6TO GRADO":["5TO GRADO","6TO GRADO"]
    }

    df["grado_detalle"] = df["grado"].map(expansion)
    df["grado_detalle"] = df["grado_detalle"].where(
        df["grado_detalle"].notna(),
        df["grado"].apply(lambda x:[x])
    )

    df = df.explode("grado_detalle")
    df = df.drop(columns=["grado"])

    df["sede"] = df["sede"].apply(normalizar)
    df["area"] = df["area"].apply(normalizar)
    df["grado_detalle"] = df["grado_detalle"].apply(normalizar)

    # VENTANILLA
    df_vent = df[df["sede"]=="VENTANILLA"].copy()
    df_otros = df[df["sede"]!="VENTANILLA"].copy()

    sedes_vent_detalle = ["01 PACHACUTEC","02 HDMP","03 SANTA ROSA"]

    replicas = []
    for s in sedes_vent_detalle:
        temp = df_vent.copy()
        temp["sede"] = s
        replicas.append(temp)

    df_vent = pd.concat(replicas, ignore_index=True)

    mapeo = {
        "PUENTE PIEDRA":"04 PTE. PIEDRA",
        "CUSCO":"05 CUSCO",
        "HUANCAVELICA":"06 HUANCAVELICA",
        "HUAROCHIRI":"07 HUAROCHIRI",
        "VMT - PARAISO":"08 VMT PARAISO"
    }

    df_otros["sede"] = df_otros["sede"].map(mapeo)

    df = pd.concat([df_vent, df_otros], ignore_index=True)

    resumen = (
        df.groupby(["sede","grado_detalle"],as_index=False)
        .agg(total_horas_semanales=("horas_semanales","sum"))
    )

    resumen["semanas_prom_bimestre"] = SEMANAS_POR_BIMESTRE
    resumen["total_horas_bimestre"] = (
        resumen["total_horas_semanales"] * SEMANAS_POR_BIMESTRE
    )

    bimestres = {
        "04 PTE. PIEDRA":4.5,
        "05 CUSCO":4.5,
        "06 HUANCAVELICA":4.5,
        "07 HUAROCHIRI":4.5,
        "08 VMT PARAISO":4.5,
        "01 PACHACUTEC":4,
        "02 HDMP":4,
        "03 SANTA ROSA":4
    }

    resumen["bimestres_anio"] = resumen["sede"].map(bimestres)
    resumen["extra_horas_anio"] = 0

    resumen["total_horas_anio"] = (
        resumen["total_horas_bimestre"] *
        resumen["bimestres_anio"]
    )

    df = df.merge(
        resumen,
        on=["sede","grado_detalle"],
        how="left"
    )

    df["ANIO"] = ANIO

    return df


# ==========================================================
# MAIN
# ==========================================================

def main():
    print("=== TRANSFORMACIÓN AFTER SCHOOL ===")

    df_final = construir_base()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("✅ Archivo generado")
    print(OUTPUT_FILE.resolve())


if __name__ == "__main__":
    main()
