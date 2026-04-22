
# -*- coding: utf-8 -*-
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
# CONFIGURACIÓN
# =============================================================================
CARPETA = Path("data/raw/2025/asistencias_extracurriculares")
CARPETA_SALIDA = Path("data/processed/2025/asistencias_extracurriculares")

EXTS = {".xlsx", ".xls", ".xlsm"}
COLUMNA_INICIO = "A"
MAX_FILAS_BUSQUEDA_HEADER = 20

SALIDA = CARPETA_SALIDA / "asistencias_extra_consolidado_kantaya.csv"
SALIDA_CALIDAD = CARPETA_SALIDA / "asistencias_extraconsolidado_kantaya_CALIDAD.xlsx"

if not CARPETA.exists():
    raise Exception(f"❌ La carpeta de entrada no existe: {CARPETA}")
else:
    print(f"📂 Procesando archivos desde: {CARPETA}")


# =============================================================================
# UTILIDADES
# =============================================================================
def norm_key(s: str) -> str:
    """Normaliza cadenas para comparaciones robustas."""
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def is_asistencia_sheet(name: str) -> bool:
    """Detecta hojas de asistencia."""
    n = norm_key(name)
    return ("asist" in n or "asistencia" in n) or ("asis" in n and "ist" in n)


def get_excel_col_letter(idx0: int) -> str:
    """Convierte índice base 0 a letra Excel."""
    n = idx0
    letters = ""
    while n >= 0:
        letters = chr(n % 26 + 65) + letters
        n = n // 26 - 1
    return letters


def parse_excel_date_like(val):
    """
    Intenta interpretar si un valor parece fecha real.
    Evita confundir enteros chicos como 1, 2, 3 con fechas.
    """
    if pd.isna(val):
        return None

    # Timestamp / datetime
    if isinstance(val, (pd.Timestamp, np.datetime64)):
        try:
            return pd.to_datetime(val)
        except Exception:
            return None

    # Serial Excel razonable
    if isinstance(val, (int, float)) and 20000 < float(val) < 60000:
        try:
            return pd.to_datetime("1899-12-30") + pd.to_timedelta(float(val), unit="D")
        except Exception:
            return None

    # String parseable
    s = str(val).strip()
    if not s:
        return None

    # Evitar interpretar etiquetas como MAR, ABR, etc.
    if norm_key(s) in {"mar", "abr", "may", "jun", "jul", "ago", "set", "sep", "oct", "nov", "dic"}:
        return None

    try:
        dt = pd.to_datetime(s, errors="raise", dayfirst=True)
        return dt
    except Exception:
        return None


def detect_fecha_cols(headers):
    """Detecta posiciones de columnas que parecen fechas."""
    fechas_idx = []
    for i, val in enumerate(headers):
        dt = parse_excel_date_like(val)
        if dt is not None:
            fechas_idx.append(i)
    return fechas_idx


def extraer_tutor(path: Path, hoja: str) -> str:
    """Extrae TUTOR/TUTORA desde las primeras filas."""
    try:
        df_header = pd.read_excel(path, sheet_name=hoja, header=None, nrows=10, engine="openpyxl")
    except Exception:
        return None

    patron = re.compile(r"tutor[a]?:", flags=re.IGNORECASE)
    for _, row in df_header.iterrows():
        for i, val in enumerate(row):
            if isinstance(val, str) and patron.search(val):
                if i + 1 < len(row):
                    nombre_tutor = str(row[i + 1]).strip()
                    if nombre_tutor and nombre_tutor.lower() != "nan":
                        return nombre_tutor
    return None


def detectar_fila_encabezado(path: Path, hoja: str, max_filas=20):
    """
    Detecta dinámicamente la fila real de encabezado.
    Busca una fila que contenga al menos DNI y GRADO, idealmente también SEXO o APELLIDOS.
    """
    try:
        preview = pd.read_excel(
            path,
            sheet_name=hoja,
            header=None,
            nrows=max_filas,
            engine="openpyxl"
        )
    except Exception as e:
        print(f"  [ERROR] No se pudo leer preview de {path.name} - {hoja}: {e}")
        return None

    for i, row in preview.iterrows():
        vals = [str(x).strip().upper() for x in row.tolist() if pd.notna(x)]

        tiene_dni = "DNI" in vals
        tiene_grado = "GRADO" in vals
        tiene_sexo = "SEXO" in vals
        tiene_nombre = any(v in vals for v in ["APELLIDOS Y NOMBRES", "NOMBRES Y APELLIDOS", "NOMBRE COMPLETO"])

        if tiene_dni and tiene_grado and (tiene_sexo or tiene_nombre):
            return i

    return None


def codebook(df):
    """Genera reporte de calidad."""
    print("   Calculando tipos, nulos y únicos...")
    resumen = pd.DataFrame({
        "Tipo": df.dtypes.astype(str),
        "Nulos (#)": df.isnull().sum(),
        "Porcentaje Nulos (%)": ((df.isnull().sum() / len(df)) * 100).round(2),
        "Valores únicos (#)": df.nunique(dropna=True),
    })

    print("   Calculando min/max numéricos...")
    resumen["Mínimo"] = df.apply(
        lambda x: x.min(skipna=True) if pd.api.types.is_numeric_dtype(x) else None
    )
    resumen["Máximo"] = df.apply(
        lambda x: x.max(skipna=True) if pd.api.types.is_numeric_dtype(x) else None
    )

    print("   Verificando valores duplicados...")
    resumen["Duplicados (Valores)"] = "No"

    if "DNI" in df.columns:
        total_dups_dni = df["DNI"].dropna().duplicated().sum()
        if total_dups_dni > 0:
            print(f"   -> ALERTA PK (DNI): {total_dups_dni} duplicados encontrados.")
            if "DNI" in resumen.index:
                resumen.loc["DNI", "Duplicados (Valores)"] = f"¡SÍ! ({total_dups_dni} duplicados)"
        else:
            print("   -> Verificación PK (DNI): OK (0 duplicados).")
            if "DNI" in resumen.index:
                resumen.loc["DNI", "Duplicados (Valores)"] = "No (PK Válida)"

    for col in df.columns:
        if col != "DNI" and df[col].duplicated().any():
            resumen.loc[col, "Duplicados (Valores)"] = "Sí"

    print("   Extrayendo muestra de valores únicos...")
    def get_unique_values(x):
        unicos = x.dropna().unique()
        if x.nunique(dropna=True) > 50:
            return f"({x.nunique(dropna=True)} valores) Ej: {str(list(unicos[:5]))}"
        return str(list(unicos))

    resumen["Valores únicos (Muestra)"] = df.apply(get_unique_values)
    resumen = resumen.reset_index().rename(columns={"index": "Variable"})
    return resumen


# =============================================================================
# LECTURA DINÁMICA
# =============================================================================
def leer_rango_dinamico(path: Path, hoja: str) -> pd.DataFrame:
    """
    Lee una hoja identificando dinámicamente:
    - la fila de encabezado real,
    - las columnas de fechas,
    - y las 3 columnas posteriores de totales.
    """
    fila_header = detectar_fila_encabezado(path, hoja, MAX_FILAS_BUSQUEDA_HEADER)

    if fila_header is None:
        print(f"  [AVISO] No se encontró fila de encabezado en {hoja}")
        return pd.DataFrame()

    print(f"      fila encabezado detectada en {hoja}: {fila_header}")

    try:
        header_row = pd.read_excel(
            path,
            sheet_name=hoja,
            header=None,
            skiprows=fila_header,
            nrows=1,
            engine="openpyxl"
        )
    except Exception as e:
        print(f"  [ERROR] Encabezado {path.name} - {hoja}: {e}")
        return pd.DataFrame()

    headers = header_row.iloc[0].tolist()
    print(f"      headers detectados en {hoja}: {headers}")

    fechas_idx = detect_fecha_cols(headers)
    print(f"      índices de fecha en {hoja}: {fechas_idx}")

    if not fechas_idx:
        print(f"  [AVISO] Sin fechas detectadas en {hoja}")
        return pd.DataFrame()

    last_date_idx = max(fechas_idx)
    col_final_idx = min(len(headers) - 1, last_date_idx + 3)
    col_final_letter = get_excel_col_letter(col_final_idx)
    rango = f"{COLUMNA_INICIO}:{col_final_letter}"

    print(f"      rango leído en {hoja}: {rango}")

    try:
        df = pd.read_excel(
            path,
            sheet_name=hoja,
            header=fila_header,
            usecols=rango,
            dtype=object,
            engine="openpyxl"
        )
        df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")

        # Eliminar columnas totalmente 'Unnamed' vacías si quedaron
        cols_validas = []
        for c in df.columns:
            if str(c).startswith("Unnamed"):
                if df[c].notna().any():
                    cols_validas.append(c)
            else:
                cols_validas.append(c)
        df = df[cols_validas]

        return df

    except Exception as e:
        print(f"  [ERROR] Datos {path.name} - {hoja}: {e}")
        return pd.DataFrame()


# =============================================================================
# NORMALIZACIÓN DE IDENTIDAD
# =============================================================================
def _clean_text_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r"\s+", " ", regex=True).str.strip()


def detectar_y_unificar_nombres(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye nombre_completo a partir de columnas disponibles.
    """
    cols = list(df.columns)
    norm_map = [(c, norm_key(c)) for c in cols]

    def _find_col(patterns):
        for orig, nk in norm_map:
            for pat in patterns:
                if re.search(pat, nk):
                    return orig
        return None

    dni_col = _find_col([r"\bdni\b", r"\bn[_\.\s]*dni\b"])
    nombres_col = _find_col([r"\bnombres?\b", r"\bnombre\b"])
    ap_pat_col = _find_col([r"\bapellido\s*paterno\b", r"\bap[e\. ]*pat(?:erno)?\b", r"\bpaterno\b"])
    ap_mat_col = _find_col([r"\bapellido\s*materno\b", r"\bap[e\. ]*mat(?:erno)?\b", r"\bmaterno\b"])
    apellidos_col = _find_col([r"\bapellidos?\b", r"\bapellidoynombres?\b", r"\bapellidosy?nombres?\b"])
    unico_nombre_col = _find_col([
        r"\bapellidos?\s*y\s*nombres?\b",
        r"\bnombres?\s*y\s*apellidos?\b",
        r"\bnombre\s*y\s*apellid",
        r"\bnombre\s*completo\b",
        r"\balumno\b",
        r"\bnombre\b"
    ])

    if nombres_col is not None and ap_pat_col is not None:
        n = _clean_text_series(df[nombres_col].fillna(""))
        ap1 = _clean_text_series(df[ap_pat_col].fillna(""))
        if ap_mat_col is not None:
            ap2 = _clean_text_series(df[ap_mat_col].fillna(""))
            nombre_completo = ap1 + " " + ap2 + ", " + n
            drop_used = [nombres_col, ap_pat_col, ap_mat_col]
        else:
            nombre_completo = ap1 + ", " + n
            drop_used = [nombres_col, ap_pat_col]

    elif apellidos_col is not None and nombres_col is not None:
        a = _clean_text_series(df[apellidos_col].fillna(""))
        n = _clean_text_series(df[nombres_col].fillna(""))
        sep_coma = a.str.contains(",", regex=False, na=False)
        nombre_completo = a.where(sep_coma, a + ", " + n)
        drop_used = [apellidos_col, nombres_col]

    else:
        if unico_nombre_col == nombres_col and (ap_pat_col is not None or apellidos_col is not None):
            unico_nombre_col = None

        if unico_nombre_col is not None:
            nombre_completo = _clean_text_series(df[unico_nombre_col].fillna(""))
            drop_used = [unico_nombre_col]
        elif nombres_col is not None:
            nombre_completo = _clean_text_series(df[nombres_col].fillna(""))
            drop_used = [nombres_col]
        elif apellidos_col is not None:
            nombre_completo = _clean_text_series(df[apellidos_col].fillna(""))
            drop_used = [apellidos_col]
        else:
            return df

    nombre_completo = (
        nombre_completo.astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.replace(r"^(.*?),\s*\1$", r"\1", regex=True)
    )

    df["nombre_completo"] = nombre_completo

    drop_candidates = set()
    for c in drop_used:
        if c is not None and c != dni_col and c in df.columns:
            drop_candidates.add(c)

    df = df.drop(columns=list(drop_candidates), errors="ignore")

    new_cols = list(df.columns)
    if "nombre_completo" in new_cols:
        new_cols.remove("nombre_completo")
        new_cols.insert(0, "nombre_completo")
    if dni_col and dni_col in new_cols:
        new_cols.remove(dni_col)
        new_cols.insert(0, dni_col)

    return df[new_cols]


# =============================================================================
# ANCHO -> LARGO
# =============================================================================
def melt_por_fechas_preservando_totales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte columnas de fechas a filas.
    Preserva hasta 3 columnas posteriores a la última fecha como totales.
    """
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

    tail_idxs = [i for i in range(last_date_pos + 1, min(last_date_pos + 4, len(cols)))]
    tail_names = [cols[i] for i in tail_idxs]

    rename_map = {}
    if len(tail_names) >= 1:
        rename_map[tail_names[0]] = "ASISTENCIAS ESPERADAS"
    if len(tail_names) >= 2:
        rename_map[tail_names[1]] = "ASISTENCIAS REALES"
    if len(tail_names) >= 3:
        rename_map[tail_names[2]] = "% DE PARTICIPACIÓN"

    if rename_map:
        df = df.rename(columns=rename_map)
        cols = list(df.columns)

    tail_final = [
        n for n in ["ASISTENCIAS ESPERADAS", "ASISTENCIAS REALES", "% DE PARTICIPACIÓN"]
        if n in df.columns
    ]

    id_vars = cols[:first_date_pos] + tail_final
    value_vars = [cols[i] for i in fecha_idx]

    m = df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="fecha",
        value_name="asistencia"
    )

    m["fecha"] = pd.to_datetime(m["fecha"], errors="coerce", dayfirst=True)
    return m


# =============================================================================
# PROCESO POR ARCHIVO
# =============================================================================
def procesar_archivo(path: Path) -> pd.DataFrame:
    try:
        xls = pd.ExcelFile(path)
    except Exception as e:
        print(f"[ERROR] Abrir {path.name}: {e}")
        return pd.DataFrame()

    print(f"\n📘 Archivo: {path.name}")
    print(f"   Hojas disponibles: {xls.sheet_names}")

    hojas = [h for h in xls.sheet_names if is_asistencia_sheet(h)]
    print(f"   Hojas de asistencia detectadas: {hojas}")

    if not hojas:
        return pd.DataFrame()

    frames = []

    for h in hojas:
        print(f"   → Procesando hoja: {h}")
        tutor = extraer_tutor(path, h)

        ancho = leer_rango_dinamico(path, h)
        print(f"      shape ancho: {ancho.shape}")

        if ancho.empty:
            print(f"      [DESCARTADA] hoja vacía o mal leída: {h}")
            continue

        print(f"      columnas ancho: {list(ancho.columns)}")

        ancho = detectar_y_unificar_nombres(ancho)
        largo = melt_por_fechas_preservando_totales(ancho)

        print(f"      shape largo: {largo.shape}")

        if largo.empty:
            print(f"      [DESCARTADA] no se pudo hacer melt: {h}")
            continue

        if "GRADO" in largo.columns:
            print(f"      grados detectados en {h}: {largo['GRADO'].dropna().astype(str).unique().tolist()}")

        largo["archivo_origen"] = path.name
        largo["hoja_origen"] = h
        largo["TUTOR"] = tutor
        frames.append(largo)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# =============================================================================
# MAIN
# =============================================================================
def main():
    excels = [
        p for p in CARPETA.rglob("*")
        if p.suffix.lower() in EXTS and not p.name.startswith("~$")
    ]

    print(f"📂 Archivos Excel encontrados: {len(excels)}")
    all_frames = []

    for f in excels:
        print(f"🔹 Procesando: {f.name}")
        df = procesar_archivo(f)
        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        print("⚠️ No se extrajo información de asistencia.")
        return

    big = pd.concat(all_frames, ignore_index=True)

    # -------------------------------------------------------------------------
    # Limpieza de DNI
    # -------------------------------------------------------------------------
    dni_cols = [c for c in big.columns if "dni" in norm_key(c)]
    if dni_cols:
        dni_col = dni_cols[0]
        before = len(big)
        big[dni_col] = (
            big[dni_col]
            .astype(str)
            .str.replace(r"\D", "", regex=True)
            .replace("", np.nan)
            .replace("nan", np.nan)
        )
        big = big[big[dni_col].notna()]
        after = len(big)
        print(f"✅ Filas sin DNI eliminadas: {before - after:,} (quedan {after:,})")
    else:
        print("⚠️ No se encontró columna 'DNI' para filtrar (se conserva todo).")

    # -------------------------------------------------------------------------
    # Uniformización
    # -------------------------------------------------------------------------
    print("🔧 Uniformizando 'SEXO', 'TIPO DE ALUMNO' y creando 'CENTRO'...")

    if "SEXO" in big.columns:
        sexo_map = {
            "M": "MASCULINO",
            "H": "MASCULINO",
            "MASCULINO": "MASCULINO",
            "F": "FEMENINO",
            "FEMENINO": "FEMENINO",
        }
        big["SEXO"] = big["SEXO"].astype(str).str.upper().str.strip().replace("NAN", np.nan)
        big["SEXO"] = big["SEXO"].map(sexo_map).fillna(big["SEXO"])

    if "TIPO DE ALUMNO" in big.columns:
        big["TIPO DE ALUMNO"] = (
            big["TIPO DE ALUMNO"]
            .astype(str)
            .str.upper()
            .str.strip()
            .replace("NAN", np.nan)
        )

    if "archivo_origen" in big.columns:
        big["CENTRO"] = (
            big["archivo_origen"]
            .str.replace(r"\.xlsx$|\.xlsm$|\.xls$", "", regex=True)
            .str.replace("TALLERES_", "", regex=False)
            .str.replace(" REGISTRO DE ASISTENCIA 2025", "", regex=False)
            .str.strip()
        )
    else:
        big["CENTRO"] = np.nan

    # -------------------------------------------------------------------------
    # Orden final
    # -------------------------------------------------------------------------
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

    cols_orden = [src for src, _ in orden_renombre]
    rename_map = {src: dst for src, dst in orden_renombre}
    big = big[cols_orden].rename(columns=rename_map)

    # -------------------------------------------------------------------------
    # Formateo de fechas
    # -------------------------------------------------------------------------
    print("🔧 Formateando columnas de fecha (F_INCORPORACION, F_SALIDA, FECHA)...")
    date_cols_to_format = ["FECHA", "F_INCORPORACION", "F_SALIDA"]

    for col in date_cols_to_format:
        if col in big.columns:
            datetime_series = pd.to_datetime(big[col], errors="coerce", dayfirst=True)
            string_series = datetime_series.dt.strftime("%d/%m/%Y")
            big[col] = string_series.replace("NaT", np.nan)

    # -------------------------------------------------------------------------
    # Logs finales de control
    # -------------------------------------------------------------------------
    if "GRADO" in big.columns:
        print("\n📌 Grados finales:")
        print(big["GRADO"].dropna().astype(str).str.upper().value_counts())

    if "HOJA_ORIGEN" in big.columns and "GRADO" in big.columns:
        print("\n📌 Conteo por hoja y grado:")
        print(big.groupby(["HOJA_ORIGEN", "GRADO"]).size().reset_index(name="filas"))

    # -------------------------------------------------------------------------
    # Reporte de calidad
    # -------------------------------------------------------------------------
    print("\n📊 Generando reporte de calidad de datos (Codebook)...")
    df_calidad = codebook(big)

    try:
        CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)
        df_calidad.to_excel(SALIDA_CALIDAD, index=False, engine="openpyxl")
        print(f"✅ Reporte de calidad guardado en:\n{SALIDA_CALIDAD}")
    except Exception as e:
        print(f"[ERROR] No se pudo guardar el reporte de calidad: {e}")

    # -------------------------------------------------------------------------
    # Exportación
    # -------------------------------------------------------------------------
    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)
    big.to_csv(SALIDA, index=False, encoding="utf-8-sig")

    print(f"\n✅ Consolidado general guardado en:\n{SALIDA}")
    print(f"   Filas: {len(big):,} | Columnas: {len(big.columns)}")


if __name__ == "__main__":
    main()
