"""
CONSOLIDACIÓN DE LISTAS DE ASISTENCIA EXTRACURRICULAR
-----------------------------------------------------
"""

import pandas as pd
import re
import unicodedata
import numpy as np
from pathlib import Path


# =============================================================================
# CONFIG
# =============================================================================
CARPETA = Path("data/raw/2025/asistencias_extracurriculares")
CARPETA_SALIDA = Path("data/processed/2025/asistencias_extracurriculares")

EXTS = {".xlsx", ".xls", ".xlsm"}

SALIDA = CARPETA_SALIDA / "asistencias_extra_consolidado_kantaya.csv"
SALIDA_CALIDAD = (
    CARPETA_SALIDA / "asistencias_extraconsolidado_kantaya_CALIDAD.xlsx"
)

MAX_FILAS_BUSQUEDA_HEADER = 20

if not CARPETA.exists():
    raise Exception(f"❌ No existe carpeta: {CARPETA}")

print(f"📂 Procesando desde: {CARPETA}")


# =============================================================================
# UTILS
# =============================================================================
def norm_key(s):
    if s is None:
        return ""

    s = str(s)

    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")

    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)

    return s.strip().lower()


def is_asistencia_sheet(name):
    n = norm_key(name)
    return "asist" in n or "asistencia" in n


def excel_col(idx):
    letters = ""
    while idx >= 0:
        letters = chr(idx % 26 + 65) + letters
        idx = idx // 26 - 1
    return letters


# =============================================================================
# FECHAS
# =============================================================================
def parse_excel_date_like(val):

    if pd.isna(val):
        return None

    # datetime directo
    if isinstance(val, (pd.Timestamp, np.datetime64)):
        try:
            return pd.to_datetime(val)
        except Exception:
            return None

    # serial excel válido
    if isinstance(val, (int, float)):
        if 20000 < float(val) < 60000:
            try:
                return (
                    pd.to_datetime("1899-12-30")
                    + pd.to_timedelta(float(val), unit="D")
                )
            except Exception:
                return None

    s = str(val).strip()

    if not s:
        return None

    meses = {
        "mar", "abr", "may", "jun", "jul",
        "ago", "set", "sep", "oct", "nov", "dic"
    }

    if norm_key(s) in meses:
        return None

    # formato yyyy-mm-dd hh:mm:ss
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        try:
            return pd.to_datetime(
                s,
                errors="raise",
                dayfirst=False
            )
        except Exception:
            return None

    try:
        return pd.to_datetime(
            s,
            errors="raise",
            dayfirst=True
        )
    except Exception:
        return None


# =============================================================================
# HEADER DETECTION
# =============================================================================
def detectar_fila_encabezado(path, hoja):

    preview = pd.read_excel(
        path,
        sheet_name=hoja,
        header=None,
        nrows=MAX_FILAS_BUSQUEDA_HEADER,
        engine="openpyxl"
    )

    for i, row in preview.iterrows():

        vals = [
            str(x).strip().upper()
            for x in row.tolist()
            if pd.notna(x)
        ]

        tiene_dni = "DNI" in vals
        tiene_grado = "GRADO" in vals

        tiene_nombre = any(
            x in vals
            for x in [
                "APELLIDOS Y NOMBRES",
                "NOMBRES Y APELLIDOS",
                "NOMBRE COMPLETO"
            ]
        )

        if tiene_dni and tiene_grado and tiene_nombre:
            return i

    return None


# =============================================================================
# TUTOR
# =============================================================================
def extraer_tutor(path, hoja):

    try:
        tmp = pd.read_excel(
            path,
            sheet_name=hoja,
            header=None,
            nrows=10,
            engine="openpyxl"
        )

    except Exception:
        return None

    patron = re.compile(r"tutor[a]?:", flags=re.IGNORECASE)

    for _, row in tmp.iterrows():

        for i, val in enumerate(row):

            if isinstance(val, str) and patron.search(val):

                if i + 1 < len(row):

                    nombre = str(row[i + 1]).strip()

                    if nombre and nombre.lower() != "nan":
                        return nombre

    return None


# =============================================================================
# LECTURA DINÁMICA
# =============================================================================
def leer_rango_dinamico(path, hoja):

    fila_header = detectar_fila_encabezado(path, hoja)

    if fila_header is None:
        print(f"  [AVISO] header no detectado: {hoja}")
        return pd.DataFrame()

    print(f"      fila header: {fila_header}")

    header_row = pd.read_excel(
        path,
        sheet_name=hoja,
        header=None,
        skiprows=fila_header,
        nrows=1,
        engine="openpyxl"
    )

    headers = header_row.iloc[0].tolist()

    fecha_idx = []

    for i, val in enumerate(headers):

        dt = parse_excel_date_like(val)

        if dt is not None:
            fecha_idx.append(i)

    if not fecha_idx:
        print(f"  [AVISO] sin fechas: {hoja}")
        return pd.DataFrame()

    last_date_idx = max(fecha_idx)

    # +3 columnas de totales
    final_idx = min(len(headers) - 1, last_date_idx + 3)

    rango = f"A:{excel_col(final_idx)}"

    print(f"      rango leído: {rango}")

    df = pd.read_excel(
        path,
        sheet_name=hoja,
        header=fila_header,
        usecols=rango,
        dtype=object,
        engine="openpyxl"
    )

    # SOLO eliminar filas vacías
    # NO eliminar columnas porque rompe índices
    df = df.dropna(axis=0, how="all")

    # normalizar unnamed
    cols = []

    for i, c in enumerate(df.columns):

        if str(c).startswith("Unnamed"):
            cols.append(f"UNNAMED_{i}")
        else:
            cols.append(c)

    df.columns = cols

    return df


# =============================================================================
# NOMBRES
# =============================================================================
def detectar_y_unificar_nombres(df):

    cols = list(df.columns)

    norm_map = [(c, norm_key(c)) for c in cols]

    def find_col(patterns):

        for orig, nk in norm_map:

            for p in patterns:

                if re.search(p, nk):
                    return orig

        return None

    dni_col = find_col([r"\bdni\b"])

    nombre_col = find_col([
        r"\bapellidos?\s*y\s*nombres?\b",
        r"\bnombres?\s*y\s*apellidos?\b",
        r"\bnombre\s*completo\b"
    ])

    if nombre_col:

        df["nombre_completo"] = (
            df[nombre_col]
            .astype(str)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

    return df


# =============================================================================
# MELT
# =============================================================================
def melt_por_fechas_preservando_totales(df):

    cols = list(df.columns)

    fecha_idx = []

    for i, c in enumerate(cols):

        dt = parse_excel_date_like(c)

        if dt is not None:
            fecha_idx.append(i)

    if not fecha_idx:
        return pd.DataFrame()

    first_date_pos = min(fecha_idx)
    last_date_pos = max(fecha_idx)

    print(f"      first_date_pos: {first_date_pos}")
    print(f"      last_date_pos: {last_date_pos}")

    # columnas fecha
    value_vars = [cols[i] for i in fecha_idx]

    # columnas posteriores
    tail_cols = []

    for i in range(last_date_pos + 1, min(last_date_pos + 4, len(cols))):
        tail_cols.append(cols[i])

    rename_tail = {}

    if len(tail_cols) >= 1:
        rename_tail[tail_cols[0]] = "ASISTENCIAS ESPERADAS"

    if len(tail_cols) >= 2:
        rename_tail[tail_cols[1]] = "ASISTENCIAS REALES"

    if len(tail_cols) >= 3:
        rename_tail[tail_cols[2]] = "% DE PARTICIPACIÓN"

    df = df.rename(columns=rename_tail)

    tail_final = [
        x for x in [
            "ASISTENCIAS ESPERADAS",
            "ASISTENCIAS REALES",
            "% DE PARTICIPACIÓN"
        ]
        if x in df.columns
    ]

    # IMPORTANTE
    # recalcular cols después rename
    cols = list(df.columns)

    id_vars = []

    for c in cols:

        if c not in value_vars:
            id_vars.append(c)

    print(f"      id_vars: {id_vars}")

    m = df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="fecha",
        value_name="asistencia"
    )

    m["fecha"] = pd.to_datetime(
        m["fecha"],
        errors="coerce"
    )

    return m


# =============================================================================
# CODEBOOK
# =============================================================================
def codebook(df):

    resumen = pd.DataFrame({
        "Tipo": df.dtypes.astype(str),
        "Nulos": df.isnull().sum(),
        "Unicos": df.nunique(dropna=True)
    })

    return resumen.reset_index().rename(
        columns={"index": "Variable"}
    )


# =============================================================================
# PROCESAMIENTO ARCHIVO
# =============================================================================
def procesar_archivo(path):

    try:
        xls = pd.ExcelFile(path)

    except Exception as e:
        print(f"[ERROR] {path.name}: {e}")
        return pd.DataFrame()

    hojas = [
        h
        for h in xls.sheet_names
        if is_asistencia_sheet(h)
    ]

    print(f"\n📘 {path.name}")
    print(f"   hojas asistencia: {hojas}")

    frames = []

    for h in hojas:

        print(f"   → {h}")

        tutor = extraer_tutor(path, h)

        ancho = leer_rango_dinamico(path, h)

        if ancho.empty:
            continue

        print(f"      shape ancho: {ancho.shape}")

        ancho = detectar_y_unificar_nombres(ancho)

        largo = melt_por_fechas_preservando_totales(ancho)

        if largo.empty:
            continue

        print(f"      shape largo: {largo.shape}")

        if "GRADO" in largo.columns:

            print(
                "      grados:",
                largo["GRADO"]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )

        largo["archivo_origen"] = path.name
        largo["hoja_origen"] = h
        largo["TUTOR"] = tutor

        frames.append(largo)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# =============================================================================
# MAIN
# =============================================================================
def main():

    excels = [
        p for p in CARPETA.rglob("*")
        if p.suffix.lower() in EXTS
        and not p.name.startswith("~$")
    ]

    print(f"📂 excels encontrados: {len(excels)}")

    all_frames = []

    for f in excels:

        print(f"\n🔹 {f.name}")

        df = procesar_archivo(f)

        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        print("⚠️ Sin datos")
        return

    big = pd.concat(all_frames, ignore_index=True)

    # =========================================================================
    # DNI
    # =========================================================================
    dni_cols = [c for c in big.columns if "dni" in norm_key(c)]

    if dni_cols:

        dni_col = dni_cols[0]

        big[dni_col] = (
            big[dni_col]
            .astype(str)
            .str.replace(r"\D", "", regex=True)
            .replace("", np.nan)
            .replace("nan", np.nan)
        )

        before = len(big)

        big = big[big[dni_col].notna()]

        after = len(big)

        print(f"✅ filas sin dni eliminadas: {before - after}")

    # =========================================================================
    # SEXO
    # =========================================================================
    if "SEXO" in big.columns:

        sexo_map = {
            "M": "MASCULINO",
            "F": "FEMENINO",
            "H": "MASCULINO"
        }

        big["SEXO"] = (
            big["SEXO"]
            .astype(str)
            .str.upper()
            .str.strip()
        )

        big["SEXO"] = (
            big["SEXO"]
            .map(sexo_map)
            .fillna(big["SEXO"])
        )

    # =========================================================================
    # CENTRO
    # =========================================================================
    big["CENTRO"] = (
        big["archivo_origen"]
        .astype(str)
        .str.replace(r"\.xlsx$|\.xlsm$|\.xls$", "", regex=True)
        .str.replace("TALLERES_", "", regex=False)
        .str.replace(" REGISTRO DE ASISTENCIA 2025", "", regex=False)
        .str.strip()
    )

    # =========================================================================
    # RENAME
    # =========================================================================
    orden_renombre = [
        ("DNI", "DNI"),
        ("nombre_completo", "NOMBRE"),
        ("CENTRO", "CENTRO"),
        ("GRADO", "GRADO"),
        ("SEXO", "SEXO"),
        ("FECHA DE INCORPORACIÓN", "F_INCORPORACION"),
        ("TIPO DE ALUMNO", "TIPO_ALUMNO"),
        ("FECHA DE SALIDA", "F_SALIDA"),
        ("ALERTAS DE ASISTENCIA", "ALERTAS_ASISTENCIA"),
        ("fecha", "FECHA"),
        ("asistencia", "ASISTENCIA"),
        ("ASISTENCIAS ESPERADAS", "A_ESPERADAS"),
        ("ASISTENCIAS REALES", "A_REALES"),
        ("% DE PARTICIPACIÓN", "PORC_PART"),
        ("archivo_origen", "ARCHIVO_ORIGEN"),
        ("hoja_origen", "HOJA_ORIGEN"),
        ("TUTOR", "TUTOR"),
    ]

    for src, _ in orden_renombre:

        if src not in big.columns:
            big[src] = pd.NA

    cols_orden = [x[0] for x in orden_renombre]

    rename_map = {
        x[0]: x[1]
        for x in orden_renombre
    }

    big = big[cols_orden].rename(columns=rename_map)

    # =========================================================================
    # FECHAS
    # =========================================================================
    for c in ["FECHA", "F_INCORPORACION", "F_SALIDA"]:

        if c in big.columns:

            dt = pd.to_datetime(
                big[c],
                errors="coerce",
                dayfirst=True
            )

            big[c] = dt.dt.strftime("%d/%m/%Y")

    # =========================================================================
    # DEBUG GRADO
    # =========================================================================
    print("\n📌 grados finales")

    print(
        big["GRADO"]
        .astype(str)
        .value_counts(dropna=False)
    )

    print("\n📌 únicos grado")

    print(
        big["GRADO"]
        .dropna()
        .astype(str)
        .unique()
    )

    print("\n📌 muestra segundo")

    print(
        big[big["GRADO"] == "SEGUNDO"]
        .head()
    )

    # =========================================================================
    # EXPORT
    # =========================================================================
    CARPETA_SALIDA.mkdir(
        parents=True,
        exist_ok=True
    )

    big.to_csv(
        SALIDA,
        index=False,
        encoding="utf-8-sig"
    )

    # VALIDACIÓN REAL CSV
    print("\n🔎 validando csv exportado")

    validacion = pd.read_csv(SALIDA)

    print(
        validacion["GRADO"]
        .value_counts(dropna=False)
    )

    df_calidad = codebook(big)

    df_calidad.to_excel(
        SALIDA_CALIDAD,
        index=False,
        engine="openpyxl"
    )

    print(f"\n✅ csv: {SALIDA}")
    print(f"✅ calidad: {SALIDA_CALIDAD}")

    print(f"\n📊 filas: {len(big):,}")


if __name__ == "__main__":
    main()
