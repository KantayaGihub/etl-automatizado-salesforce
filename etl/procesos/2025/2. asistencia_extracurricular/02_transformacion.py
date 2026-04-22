# -*- coding: utf-8 -*-
"""
CONSOLIDACIÓN DE LISTAS DE ASISTENCIA (Kantaya)
-----------------------------------------------
"""


# === LIBRERÍAS ===============================================================
import pandas as pd
import re
import unicodedata
import numpy as np
from pathlib import Path

# === CONFIGURACIÓN DE RUTAS (CORREGIDO) ====================================

CARPETA = Path("data/raw/2025/asistencias_extracurriculares")
CARPETA_SALIDA = Path("data/processed/2025/asistencias_extracurriculares")

if not CARPETA.exists():
    raise Exception(f"❌ La carpeta de entrada no existe: {CARPETA}")
else:
    print(f"📂 Procesando archivos desde: {CARPETA}")

# Parámetros de lectura por hoja
FILA_ENCABEZADO = 6      # C7 ⇒ fila 7 en Excel; pandas usa base 0, por eso header=6
COLUMNA_INICIO = "C"     # Punto de partida (C); el rango se expandirá dinámicamente
EXTS = {".xlsx", ".xls", ".xlsm"}  # Extensiones válidas
SALIDA = CARPETA_SALIDA / "asistencias_extra_consolidado_kantaya.csv"  # Nombre del CSV de salida
SALIDA_CALIDAD = CARPETA_SALIDA / "asistencias_extraconsolidado_kantaya_CALIDAD.xlsx" # <-- NUEVO

# ---------- UTILIDADES -------------------------------------------------------
def norm_key(s: str) -> str:
    """
    Normaliza cadenas para comparaciones robustas:
    - Quita tildes (NFKD),
    - Pasa a minúsculas,
    - Elimina símbolos,
    - Colapsa espacios a uno solo.
    """
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def is_asistencia_sheet(name: str) -> bool:
    """
    Determina si el nombre de la hoja corresponde a 'asistencia' o similar.
    Admite abreviaturas y errores de tipeo simples (p. ej., 'asis' + 'ist').
    """
    n = norm_key(name)
    return ("asist" in n or "asistencia" in n) or ("asis" in n and "ist" in n)

def detect_fecha_cols(headers):
    """
    Detecta posiciones de columnas de fecha en un encabezado.
    Considera:
      - objetos datetime/np.datetime64,
      - seriales Excel (rango aproximado 20000–60000),
      - strings parseables como fecha (dayfirst=True).
    Devuelve: lista de índices de columnas que parecen fechas.
    """
    fechas_idx = []
    for i, val in enumerate(headers):
        # Caso: tipo datetime-like
        if pd.api.types.is_datetime64_any_dtype(type(val)):
            fechas_idx.append(i)
            continue
        # Caso: serial Excel (días desde 1899)
        if isinstance(val, (int, float)) and 20000 < val < 60000:
            fechas_idx.append(i)
            continue
        # Caso: cadena de fecha
        try:
            pd.to_datetime(str(val), errors="raise", dayfirst=True)
            fechas_idx.append(i)
        except Exception:
            pass
    return fechas_idx

def get_excel_col_letter(idx0: int) -> str:
    """
    Convierte índice base 0 a letra de columna Excel:
    0→A, 1→B, ..., 25→Z, 26→AA, etc.
    """
    n = idx0
    letters = ""
    while n >= 0:
        letters = chr(n % 26 + 65) + letters
        n = n // 26 - 1
    return letters

def extraer_tutor(path: Path, hoja: str) -> str:
    """
    Intenta detectar 'TUTOR:' o 'TUTORA:' en las primeras filas de la hoja y
    devuelve la celda contigua a la derecha como nombre del tutor.
    Si no se encuentra, retorna None. Límite: primeras 10 filas.
    """
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

def codebook(df):
    """
    Genera un DataFrame de resumen (codebook) sobre la calidad de datos
    del DataFrame final.
    """
    print("   Calculando tipos, nulos y únicos...")
    resumen = pd.DataFrame({
        "Tipo": df.dtypes,
        "Nulos (#)": df.isnull().sum(),
        "Porcentaje Nulos (%)": ((df.isnull().sum() / len(df)) * 100),
        "Valores únicos (#)": df.nunique(),
    })

    print("   Calculando min/max numéricos...")
    # Usar skipna=True para ignorar nulos en min/max
    resumen["Mínimo"] = df.apply(lambda x: x.min(skipna=True) if pd.api.types.is_numeric_dtype(x) else None)
    resumen["Máximo"] = df.apply(lambda x: x.max(skipna=True) if pd.api.types.is_numeric_dtype(x) else None)

    print("   Verificando valores duplicados (dentro de la columna)...")

    # --- MODIFICACIÓN: Chequeo de PK (DNI) ---
    resumen["Duplicados (Valores)"] = "No" # Default
    if "DNI" in df.columns:
        # CORRECCIÓN: El chequeo de duplicados debe ser en el DNI limpio
        # (Aunque en este punto del flujo, el codebook recibe el DF ya filtrado)
        total_dups_dni = df['DNI'].dropna().duplicated().sum()
        if total_dups_dni > 0:
            print(f"   -> ALERTA PK (DNI): {total_dups_dni} duplicados encontrados.")
            resumen.loc["DNI", "Duplicados (Valores)"] = f"¡SÍ! ({total_dups_dni} duplicados)"
        else:
            print("   -> Verificación PK (DNI): OK (0 duplicados).")
            resumen.loc["DNI", "Duplicados (Valores)"] = "No (PK Válida)"

    # Chequear duplicados para otras columnas
    for col in df.columns:
        if col != "DNI" and df[col].duplicated().any():
            resumen.loc[col, "Duplicados (Valores)"] = "Sí"
    # --- FIN MODIFICACIÓN ---

    print("   Extrayendo muestra de valores únicos (límite 50)...")
    def get_unique_values(x):
        unicos = x.dropna().unique()
        if x.nunique() > 50:
            # Si hay demasiados, solo mostrar los primeros 5 como ejemplo
            return f"({x.nunique()} valores) Ej: {str(list(unicos[:5]))}"
        else:
            # Si son 50 o menos, mostrar todos
            return str(list(unicos))

    resumen["Valores únicos (Muestra)"] = df.apply(get_unique_values)

    resumen = resumen.reset_index().rename(columns={"index": "Variable"})
    return resumen

# ---------- LECTURA DINÁMICA -------------------------------------------------
def leer_rango_dinamico(path: Path, hoja: str) -> pd.DataFrame:
    try:
        header_row = pd.read_excel(
            path, sheet_name=hoja, header=None, skiprows=FILA_ENCABEZADO, nrows=1, engine="openpyxl"
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
            path, sheet_name=hoja, header=FILA_ENCABEZADO, usecols=rango, dtype=object, engine="openpyxl"
        )
        df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
        return df
    except Exception as e:
        print(f"  [ERROR] Datos {path.name} - {hoja}: {e}")
        return pd.DataFrame()

# ---------- NORMALIZACIÓN DE IDENTIDAD --------------------------------------
def _clean_text_series(s: pd.Series) -> pd.Series:
    """Limpia textos básicos: colapsa espacios y aplica strip()."""
    return s.astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

def detectar_y_unificar_nombres(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye 'nombre_completo' con prioridad:
      a) NOMBRES + APELLIDO PATERNO (+ APELLIDO MATERNO),
      b) APELLIDOS + NOMBRES,
      c) Columna única de nombre.
    Elimina columnas intermedias usadas (excepto DNI) y reordena
    para colocar primero DNI y NOMBRE_COMPLETO.
    """
    cols = list(df.columns)
    norm_map = [(c, norm_key(c)) for c in cols]

    def _find_col(patterns):
        for orig, nk in norm_map:
            for pat in patterns:
                if re.search(pat, nk):
                    return orig
        return None

    # Posibles columnas
    dni_col       = _find_col([r"\bdni\b", r"\bn[_\.\s]*dni\b"])
    nombres_col   = _find_col([r"\bnombres?\b", r"\bnombre\b"])
    ap_pat_col    = _find_col([r"\bapellido\s*paterno\b", r"\bap[e\. ]*pat(?:erno)?\b", r"\bpaterno\b"])
    ap_mat_col    = _find_col([r"\bapellido\s*materno\b", r"\bap[e\. ]*mat(?:erno)?\b", r"\bmaterno\b"])
    apellidos_col = _find_col([r"\bapellidos?\b", r"\bapellidoynombres?\b", r"\bapellidosy?nombres?\b"])
    unico_nombre_col = _find_col([
        r"\bapellidos?\s*y\s*nombres?\b",
        r"\bnombres?\s*y\s*apellidos?\b",
        r"\bnombre\s*y\s*apellid",
        r"\bnombre\s*completo\b",
        r"\balumno\b",
        r"\bnombre\b"
    ])

    # a) NOMBRES + APELLIDO(S)
    if nombres_col is not None and ap_pat_col is not None:
        n   = _clean_text_series(df[nombres_col].fillna(""))
        ap1 = _clean_text_series(df[ap_pat_col].fillna(""))
        if ap_mat_col is not None:
            ap2 = _clean_text_series(df[ap_mat_col].fillna(""))
            nombre_completo = (ap1 + " " + ap2 + ", " + n)
            drop_used = [nombres_col, ap_pat_col, ap_mat_col]
        else:
            nombre_completo = (ap1 + ", " + n)
            drop_used = [nombres_col, ap_pat_col]

    # b) APELLIDOS + NOMBRES
    elif apellidos_col is not None and nombres_col is not None:
        a = _clean_text_series(df[apellidos_col].fillna(""))
        n = _clean_text_series(df[nombres_col].fillna(""))
        sep_coma = a.str.contains(",", regex=False, na=False)  # si ya viene "Apellidos, Nombres"
        nombre_completo = a.where(sep_coma, a + ", " + n)
        drop_used = [apellidos_col, nombres_col]

    # c) Columna única de nombre (o fallback)
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
            # Sin patrones reconocibles: no modificar estructura
            return df

    # Evitar duplicados "X, X" y aplicar limpieza final
    nombre_completo = (
        nombre_completo.astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.replace(r"^(.*?),\s*\1$", r"\1", regex=True)
    )
    df["nombre_completo"] = nombre_completo

    # Eliminar columnas usadas (excepto DNI)
    drop_candidates = set()
    for c in drop_used:
        if c is not None and c != dni_col and c in df.columns:
            drop_candidates.add(c)
    df = df.drop(columns=list(drop_candidates), errors="ignore")

    # Reordenar: primero DNI y NOMBRE
    new_cols = list(df.columns)
    if "nombre_completo" in new_cols:
        new_cols.remove("nombre_completo")
        new_cols.insert(0, "nombre_completo")
    if dni_col and dni_col in new_cols:
        new_cols.remove(dni_col)
        new_cols.insert(0, dni_col)
    df = df[new_cols]
    return df

# ---------- UNPIVOT (ANCHO → LARGO) -----------------------------------------
def melt_por_fechas_preservando_totales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte las columnas de fechas a filas y, si existen, renombra y preserva
    las 3 columnas posteriores a la última fecha como:
      - "ASISTENCIAS ESPERADAS", "ASISTENCIAS REALES", "% DE PARTICIPACIÓN".
    *No* descarta alumnos sin marcaciones de asistencia si tienen DNI.
    """
    cols = list(df.columns)

    # 1) Detectar posiciones de columnas de fecha
    fecha_idx = []
    for i, c in enumerate(cols):
        try:
            pd.to_datetime(str(c), errors="raise", dayfirst=True)
            fecha_idx.append(i)
        except Exception:
            pass
    if not fecha_idx:
        return pd.DataFrame()

    # 2) Definir tramo de fechas y 3 columnas posteriores
    first_date_pos = min(fecha_idx)
    last_date_pos  = max(fecha_idx)
    tail_idxs  = [i for i in range(last_date_pos + 1, min(last_date_pos + 4, len(cols)))]
    tail_names = [cols[i] for i in tail_idxs]

    # 3) Renombrar totales si existen
    rename_map = {}
    if len(tail_names) >= 1: rename_map[tail_names[0]] = "ASISTENCIAS ESPERADAS"
    if len(tail_names) >= 2: rename_map[tail_names[1]] = "ASISTENCIAS REALES"
    if len(tail_names) >= 3: rename_map[tail_names[2]] = "% DE PARTICIPACIÓN"
    if rename_map:
        df = df.rename(columns=rename_map)
        cols = list(df.columns)

    tail_final = [n for n in ["ASISTENCIAS ESPERADAS", "ASISTENCIAS REALES", "% DE PARTICIPACIÓN"] if n in df.columns]

    # 4) Melt: id_vars = columnas antes de la primera fecha + totales
    id_vars    = cols[:first_date_pos] + tail_final
    value_vars = [cols[i] for i in fecha_idx]
    m = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="fecha", value_name="asistencia")

    # 5) Convertir 'fecha' a datetime (para luego formatear dd/mm/yyyy)
    m["fecha"] = pd.to_datetime(m["fecha"], errors="coerce", dayfirst=True)
    return m

# ---------- PROCESO POR ARCHIVO ---------------------------------------------
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

# ---------- MAIN -------------------------------------------------------------
def main():
    """
    Flujo principal:
      1) Descubre y procesa todos los Excel de la carpeta.
      2) Consolida por hoja/archivo.
      3) Elimina filas SIN DNI (si existe la columna).
      4) <-- NUEVO: Uniformiza SEXO, TIPO_ALUMNO y crea CENTRO.
      5) Ordena/renombra columnas a un esquema único.
      6) Formatea la FECHA (dd/mm/yyyy).
      7) Genera reporte de calidad.
      8) Exporta a CSV con UTF-8 BOM.
    """
    # 1) Descubrimiento de libros válidos (omite ~$. temporales)
    excels = [p for p in CARPETA.rglob("*") if p.suffix.lower() in EXTS and not p.name.startswith("~$")]
    print(f"📂 Archivos Excel encontrados: {len(excels)}")
    all_frames = []

    # 2) Procesamiento por archivo
    for f in excels:
        print(f"🔹 Procesando: {f.name}")
        df = procesar_archivo(f)
        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        print("⚠️ No se extrajo información de asistencia.")
        return

    # 3) Consolidación global
    big = pd.concat(all_frames, ignore_index=True)

    # 4) Filtrado de filas SIN DNI (se mantiene a quienes sí tienen DNI)
    dni_cols = [c for c in big.columns if "dni" in norm_key(c)]
    if dni_cols:
        dni_col = dni_cols[0]
        before = len(big)
        # --- CORRECCIÓN: Limpiar DNI antes de filtrar ---
        # Asegura que " " o "nan" se eliminen
        big[dni_col] = big[dni_col].astype(str).str.replace(r'\D', '', regex=True).replace('', np.nan).replace('nan', np.nan)
        big = big[big[dni_col].notna()]
        after = len(big)
        print(f"✅ Filas sin DNI eliminadas: {before - after:,} (quedan {after:,})")
    else:
        print("⚠️ No se encontró columna 'DNI' para filtrar (se conserva todo).")

    # --- NUEVO: PASO 4.5 - Uniformización y Creación de Columnas ---
    print("🔧 Uniformizando 'SEXO', 'TIPO DE ALUMNO' y creando 'CENTRO'...")

    # [A] Normalizar SEXO
    if "SEXO" in big.columns:
        sexo_map = {
            'M': 'MASCULINO', 'H': 'MASCULINO', 'MASCULINO': 'MASCULINO',
            'F': 'FEMENINO', 'M': 'FEMENINO', 'FEMENINO': 'FEMENINO' # Asumiendo M=Mujer
        }
        # Normalizar a mayúsculas y aplicar mapa
        big['SEXO'] = big['SEXO'].astype(str).str.upper().str.strip().replace('NAN', np.nan)
        big['SEXO'] = big['SEXO'].map(sexo_map).fillna(big['SEXO']) # Mapea y conserva los que no coinciden

    # [B] Normalizar TIPO_ALUMNO
    if "TIPO DE ALUMNO" in big.columns:
        # (El usuario dijo que tiene 4 valores, solo limpiamos)
        big['TIPO DE ALUMNO'] = big['TIPO DE ALUMNO'].astype(str).str.upper().str.strip().replace('NAN', np.nan)

    # [C] Crear CENTRO desde archivo_origen
    if "archivo_origen" in big.columns:
        # Extraer la parte antes de ' - ' (ej. '01 PACHACÚTEC')
        big['CENTRO'] = big['archivo_origen'].str.split(' - ').str[0].str.strip()
    else:
        big['CENTRO'] = np.nan # Asegurar que la columna exista
    # --- FIN NUEVO PASO ---

    # 5) Orden y renombrado final
    orden_renombre = [
        ("DNI",                         "DNI"),
        ("nombre_completo",            "NOMBRE"),
        ("CENTRO",                     "CENTRO"), # <-- AÑADIDO
        ("GRADO",                      "GRADO"),
        ("SEXO",                       "SEXO"), # <-- AHORA ESTARÁ LIMPIO
        ("FECHA DE INCORPORACIÓN",     "F_INCORPORACION"),
        ("TIPO DE ALUMNO",             "TIPO_ALUMNO"), # <-- AHORA ESTARÁ LIMPIO
        ("FECHA DE SALIDA",            "F_SALIDA"),
        ("ALERTAS DE ASISTENCIA",      "ALERTAS_ASISTENCIA"),
        ("fecha",                      "FECHA"),
        ("asistencia",                 "ASISTENCIA"),
        ("ASISTENCIAS ESPERADAS",      "A_ESPERADAS"),
        ("ASISTENCIAS REALES",         "A_REALES"),
        ("% DE PARTICIPACIÓN",         "PORC_PART"),
        ("archivo_origen",             "ARCHIVO_ORIGEN"),
        ("hoja_origen",                "HOJA_ORIGEN"),
        ("TUTOR",                      "TUTOR"),
    ]

    # Asegurar columnas ausentes, aplicar orden y renombrar
    for src, _ in orden_renombre:
        if src not in big.columns:
            big[src] = pd.NA
    cols_orden = [src for src, _ in orden_renombre]
    rename_map = {src: dst for src, dst in orden_renombre}
    big = big[cols_orden].rename(columns=rename_map)

    # 6) Formato de fecha dd/mm/yyyy (y coerción de errores)
    print("🔧 Formateando columnas de fecha (F_INCORPORACION, F_SALIDA, FECHA)...")

    # Columnas a formatear (usa los nombres finales del DataFrame)
    date_cols_to_format = ["FECHA", "F_INCORPORACION", "F_SALIDA"]

    for col in date_cols_to_format:
        if col in big.columns:
            # 1. Convertir a datetime. Errores (strings) se vuelven NaT (Nulo)
            datetime_series = pd.to_datetime(big[col], errors="coerce", dayfirst=True)

            # 2. Formatear a string dd/mm/yyyy. NaT se convierte al string 'NaT'.
            string_series = datetime_series.dt.strftime("%d/%m/%Y")

            # 3. REEMPLAZAR el string 'NaT' por un nulo real (np.nan) para un CSV limpio.
            big[col] = string_series.replace("NaT", np.nan)
        else:
            # Imprimir aviso si una columna esperada no se encuentra
            print(f"   (Aviso: Columna {col} no encontrada para formateo de fecha)")


    # --- NUEVO: PASO 7 - Generar Reporte de Calidad ----------------------
    print("\n📊 Generando reporte de calidad de datos (Codebook)...")

    # Llamar a la función codebook con el DataFrame final
    df_calidad = codebook(big)

    # Guardar el reporte de calidad en un Excel
    try:
        # Asegurarse de que la carpeta de salida exista
        CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)
        df_calidad.to_excel(SALIDA_CALIDAD, index=False, engine="openpyxl")
        print(f"✅ Reporte de calidad guardado en:\n{SALIDA_CALIDAD}")
    except Exception as e:
        print(f"[ERROR] No se pudo guardar el reporte de calidad: {e}")
    # ---------------------------------------------------------------------

    # 8) Exportación con UTF-8 BOM (antes 7)
    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)
    big.to_csv(SALIDA, index=False, encoding="utf-8-sig")

    # Resumen
    print(f"\n✅ Consolidado general guardado en:\n{SALIDA}")
    print(f"   Filas: {len(big):,} | Columnas: {len(big.columns)}")

# Punto de entrada
if __name__ == "__main__":
    main()



