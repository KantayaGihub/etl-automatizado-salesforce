# -*- coding: utf-8 -*-
"""
SCRIPT DE CONSOLIDACIÓN DE MATRÍCULAS
--------------------------------------

📘 Descripción general
----------------------
Este script permite consolidar múltiples hojas de un archivo Excel en una sola base de datos
limpia y estandarizada. Fue diseñado para integrarse dentro de procesos de control o
seguimiento de matrículas, evaluaciones u otros registros administrativos de centros educativos.

📋 Funcionalidades principales
------------------------------
✔ Procesa automáticamente todas las hojas del archivo cuyo nombre empiece con 01, 02, 03, etc.
✔ Lee una única tabla por hoja, iniciando en la celda C5 (header=4 en Python).
✔ Limpia encabezados y datos eliminando:
    - Tabulaciones (\t)
    - Saltos de línea (\r, \n)
    - Espacios múltiples
    - Paréntesis y asteriscos en encabezados (“(*)”)
    - Tildes y diferencias de mayúsculas/minúsculas
✔ Unifica todas las hojas en una sola tabla:
    - Toma como referencia el grupo de hojas con la misma cantidad de columnas (estructura base).
    - Agrega las columnas adicionales de otras hojas al final, manteniendo la consistencia.
✔ Elimina:
    - Filas sin valor en la columna “DNI”.
    - Filas que contienen encabezados repetidos dentro de los datos.
✔ Uniformiza campos clave (GRADO, SEXO, CENTRO, CONDICION ACTUAL, FECHA DE REGISTRO).
✔ Exporta dos archivos:
    1. Un CSV limpio con separador de comas.
    2. Un reporte de calidad de datos (Codebook) en formato Excel.

👤 Autor: Christian Rodriguez
🗓️ Versión: Final (Actualizada v1.7) – Noviembre 2025
"""
# === LIBRERÍAS ===============================================================
import pandas as pd
import re
from collections import Counter
import unicodedata  # Para quitar tildes
import numpy as np   # Para valores nulos
from pathlib import Path


# 4. Configuramos el script para usar este archivo
ARCHIVO = Path("entrada/Consolidado_Matricula_AfterSchool.xlsx")

# 5. Configuración de SALIDA (Igual que antes)
CARPETA_SALIDA = Path("salida/matricula")
CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)

SALIDA = CARPETA_SALIDA / "consolidado_matricula_afterschool_2025_UNICO.csv"
SALIDA_CALIDAD = CARPETA_SALIDA / "consolidado_matricula_afterschool_2025_CALIDAD.xlsx"

# === FUNCIONES DE APOYO ======================================================
def _excel_error_tokens():
    """Define las cadenas de texto que representan errores comunes en Excel."""
    return ["#N/A", "#NA", "#DIV/0!", "#VALUE!", "#REF!", "#NAME?", "#NULL!", "#NUM!"]

def hoja_valida(nombre: str) -> bool:
    """Identifica hojas válidas cuyo nombre comienza con un número (01, 02, 03...)."""
    return bool(re.match(r"^\s*\d{1,2}", str(nombre)))

def _norm_text(s: str) -> str:
    """Normaliza texto: sin tildes, espacios colapsados, mayúsculas y guiones normalizados."""
    if s is None or pd.isna(s):
        return ""
    s = str(s)
    # Quitar tildes (ej. CONDICIÓN -> CONDICION)
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
    # Normalizar guiones (largo, etc. -> corto)
    s = s.replace("–", "-").replace("—", "-")
    # Colapsar espacios y pasar a mayúsculas
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s

def _norm_key(s: str) -> str:
    """Normaliza para clave: sin tildes, mayúsculas, espacios/guiones/underscores -> espacio simple."""
    if s is None or pd.isna(s):
        return ""
    s = str(s)
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
    s = re.sub(r"[\s_]+", " ", s) # espacios y underscores
    s = s.replace("–", "-").replace("—", "-") # guiones
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s

def _find_col(df: pd.DataFrame, candidate_names: list[str]) -> str | None:
    """
    Busca una columna en df que coincida con alguna de las variantes dadas,
    tolerando tildes, underscores y espacios múltiples.
    """
    # Usamos los encabezados ya limpios (sin tildes, MAYUSCULAS)
    norm_map = {_norm_key(c): c for c in df.columns}
    for nm in candidate_names:
        k = _norm_key(nm)
        if k in norm_map:
            return norm_map[k]
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
    resumen["Mínimo"] = df.apply(lambda x: x.min(skipna=True) if pd.api.types.is_numeric_dtype(x) else None)
    resumen["Máximo"] = df.apply(lambda x: x.max(skipna=True) if pd.api.types.is_numeric_dtype(x) else None)

    print("   Verificando valores duplicados (dentro de la columna)...")
    resumen["Duplicados (Valores)"] = "No" # Default

    # Identificar la columna DNI (buscando 'DNI DEL NIÑO' o 'DNI')
    col_dni_pk = _find_col(df, ['DNI DEL NIÑO', 'DNI']) # Usa la función robusta

    if col_dni_pk:
        total_dups_dni = df[col_dni_pk].dropna().duplicated().sum()
        if total_dups_dni > 0:
            print(f"   -> ALERTA PK ({col_dni_pk}): {total_dups_dni} duplicados encontrados.")
            resumen.loc[col_dni_pk, "Duplicados (Valores)"] = f"¡SÍ! ({total_dups_dni} duplicados)"
        else:
            print(f"   -> Verificación PK ({col_dni_pk}): OK (0 duplicados).")
            resumen.loc[col_dni_pk, "Duplicados (Valores)"] = "No (PK Válida)"

    # Chequear duplicados para otras columnas
    for col in df.columns:
        if col != col_dni_pk and df[col].duplicated().any():
            resumen.loc[col, "Duplicados (Valores)"] = "Sí"

    print("   Extrayendo muestra de valores únicos (límite 50)...")
    def get_unique_values(x):
        unicos = x.dropna().unique()
        if x.nunique() > 50:
            return f"({x.nunique()} valores) Ej: {str(list(unicos[:5]))}"
        else:
            return str(list(unicos))

    resumen["Valores únicos (Muestra)"] = df.apply(get_unique_values)

    resumen = resumen.reset_index().rename(columns={"index": "Variable"})
    return resumen

# --- NUEVO: FUNCIÓN PARA TRANSFORMAR FECHA DE REGISTRO ---
def transformar_fecha_registro(valor):
    """
    Toma un valor de la columna 'FECHA DE REGISTRO' y lo transforma
    a una fecha válida (como objeto datetime) o a nulo (NaT).
    """
    # --- PASO 1: Intentar convertir el valor a fecha ---
    fecha_dt = pd.to_datetime(valor, errors='coerce', dayfirst=True)

    if pd.notna(fecha_dt):
        return fecha_dt

    # --- PASO 2: Si la conversión falló, es un string especial ---
    if not isinstance(valor, str):
        return np.nan

    s = valor.lower().strip()

    # --- PASO 3: Aplicar las reglas de mapeo ---
    if s == '<na>':
        return np.nan

    if s == 'set24':
        return pd.Timestamp(2024, 9, 1)

    if s == 'abr - ago':
        return pd.Timestamp(2025, 4, 1)

    meses_map_2025 = {
        'agosto': 8, 'septiembre': 9, 'octubre': 10, 'abr': 4,
        'enero': 1, 'febrero': 2, 'marzo': 3, 'mayo': 5,
        'junio': 6, 'julio': 7, 'noviembre': 11, 'diciembre': 12
    }

    if s in meses_map_2025:
        mes_num = meses_map_2025[s]
        return pd.Timestamp(2025, mes_num, 1)

    return np.nan
# --- FIN FUNCIÓN ---

# === LIMPIEZA DE ENCABEZADOS Y CELDAS =======================================
def _limpiar_encabezados(cols):
    """
    Estandariza los encabezados de columnas:
    - Elimina tabulaciones, saltos y espacios innecesarios.
    - Elimina los símbolos “(*)” y asteriscos.
    - Quita tildes y pasa a MAYÚSCULAS.
    - Aplica strip() para evitar espacios al inicio o final.
    """
    limpias = []
    for c in cols:
        s = "" if pd.isna(c) else str(c)
        s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")
        s = re.sub(r"\(\s*\*\s*\)", "", s)   # elimina "(*)"
        s = s.replace("*", "")
        # --- CORRECCIÓN AQUÍ: Aplicar _norm_key para normalizar todo ---
        s = _norm_key(s)
        limpias.append(s)
    return limpias

def _limpiar_celdas_todas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza integral de celdas:
    - Elimina tabulaciones, saltos de línea y espacios múltiples.
    - Aplica strip() a todos los valores.
    - Normaliza literales vacíos a valores nulos (NA).
    """
    if df.empty:
        return df

    # --- MODIFICACIÓN: No aplicar a columnas de fecha si ya existen ---
    # Esto evita que las fechas ya convertidas se traten como texto.
    cols_a_limpiar = df.select_dtypes(include=['object']).columns

    for col in cols_a_limpiar:
        df[col] = df[col].astype(str)
        df[col] = df[col].str.replace("\t", " ", regex=False)
        df[col] = df[col].str.replace("\r", " ", regex=False).str.replace("\n", " ", regex=False)
        df[col] = df[col].str.replace(r"\s+", " ", regex=True).str.strip()
        df[col] = df[col].replace({"nan": pd.NA, "NaN": pd.NA, "None": pd.NA, "": pd.NA, "NA": pd.NA})
    return df

def _limpiar_errores_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reemplaza los errores de Excel por valores nulos (NA)
    y elimina filas o columnas completamente vacías.
    """
    if df.empty:
        return df
    df = df.replace(_excel_error_tokens(), pd.NA)
    df = df.replace(to_replace=r"^(nan|NaN|None|null|\s+)$", value=pd.NA, regex=True)
    df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
    return df

# === LECTURA Y PROCESAMIENTO DE CADA HOJA ===================================
def leer_tabla_C5(archivo: Path, hoja: str) -> pd.DataFrame:
    """
    Lee una tabla a partir de la celda C5 (header=4) en una hoja determinada.
    Incluye limpieza de errores, encabezados y contenido.
    """
    try:
        df_full = pd.read_excel(
            archivo,
            sheet_name=hoja,
            header=4,
            dtype=object,
            engine="openpyxl",
            na_values=_excel_error_tokens(),
            keep_default_na=True
        )
    except Exception as e:
        print(f"   [ERROR] No se pudo leer la hoja {hoja}: {e}")
        return pd.DataFrame()

    if df_full.shape[1] <= 2:
        return pd.DataFrame()

    # Desde C (índice 2) hacia la derecha
    df = df_full.iloc[:, 2:].copy()
    df = _limpiar_errores_excel(df)
    if df.empty:
        return pd.DataFrame()

    # Encabezados NORMALIZADOS (sin tildes, MAYÚSCULAS, guiones norm)
    df.columns = _limpiar_encabezados(df.columns)
    df = _limpiar_celdas_todas(df)
    df = df.dropna(how="all")
    return df

# === RENOMBRADO, FILTROS Y ELIMINACIÓN ======================================
def renombrar_por_posicion(df: pd.DataFrame, columnas_base: list[str]) -> pd.DataFrame:
    """Renombra las columnas según su posición si coincide con la estructura base."""
    if df.shape[1] != len(columnas_base):
        return df
    out = df.copy()
    out.columns = columnas_base
    return out

def filtrar_sin_dni(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra y elimina las filas que no contienen valores en la columna 'DNI'."""
    if df.empty:
        return df

    # Buscar 'DNI DEL NIÑO' o 'DNI' (con encabezados ya en MAYÚSCULAS y norm)
    col_dni = _find_col(df, ['DNI DEL NIÑO', 'DNI DEL NINO', 'DNI'])

    if not col_dni:
        print("⚠️ No se encontró columna 'DNI' o 'DNI DEL NIÑO'; no se aplicará filtro de filas.")
        return df

    # Limpiar DNI antes de filtrar (eliminar caracteres no numéricos y '0' exacto)
    df[col_dni] = df[col_dni].apply(
    lambda x: str(int(x)) if isinstance(x, float) and not pd.isna(x)
    else re.sub(r"\D", "", str(x)) if pd.notna(x)
    else np.nan)

    df = df[~df[col_dni].isna()]
    df = df[df[col_dni].astype(str).str.strip() != ""]
    return df

def eliminar_filas_con_encabezados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identifica y elimina filas que contienen encabezados repetidos dentro del cuerpo de los datos.
    Criterio:
        - Si al menos el 50% de los valores de la fila coinciden con los nombres de columnas,
          se considera una fila de encabezado duplicado.
    """
    if df.empty:
        return df

    # Columnas ya están limpias y en mayúsculas
    columnas_upper = [str(c) for c in df.columns]
    keep_mask = []

    for _, fila in df.iterrows():
        # Limpiar valores de la fila de la misma manera que los encabezados
        valores = [_norm_key(v) if pd.notna(v) else "" for v in fila.values]
        coincidencias = sum(1 for v in valores if v in columnas_upper and v != "")
        keep_mask.append(coincidencias < max(1, int(len(columnas_upper) * 0.5)))

    df_filtrado = df.loc[keep_mask].copy()
    eliminadas = len(df) - len(df_filtrado)
    if eliminadas > 0:
        print(f"🧹 Se eliminaron {eliminadas} filas que contenían encabezados repetidos.")
    return df_filtrado

# === PROCESO PRINCIPAL =======================================================
def main():
    """
    Función principal de consolidación:
    1️⃣ Lee todas las hojas válidas del archivo Excel.
    2️⃣ Limpia, normaliza y alinea sus estructuras.
    3️⃣ Consolida toda la información en una sola tabla.
    4️⃣ Aplica filtros finales (sin DNI, sin encabezados repetidos).
    5️⃣ Aplica uniformización (Grado, Sexo, Centro, Condición, Fecha Registro).
    6️⃣ Genera reporte de calidad (Codebook).
    7️⃣ Exporta el resultado a un archivo CSV limpio.
    """
    try:
        xls = pd.ExcelFile(ARCHIVO)
        hojas_validas = [h for h in xls.sheet_names if hoja_valida(h)]
        print(f"📘 Archivo: {ARCHIVO.name}")
        print(f"📄 Hojas totales: {len(xls.sheet_names)} | ✅ Válidas: {len(hojas_validas)}")
    except Exception as e:
        print(f"[ERROR] No se pudo abrir el archivo: {e}")
        return

    info = []
    for hoja in hojas_validas:
        print(f"🔹 Leyendo hoja: {hoja}")
        df = leer_tabla_C5(ARCHIVO, hoja)
        if df.empty:
            print("   ⚠️ Hoja sin datos desde C5 (omitida).")
            continue
        info.append((hoja, df, df.shape[1]))

    if not info:
        print("⚠️ No se encontraron tablas válidas para consolidar.")
        return

    # Determina la estructura base por cantidad de columnas
    distrib = Counter(n for _, _, n in info)
    ncols_base = max(distrib.items(), key=lambda x: x[1])[0]
    print("\n📊 Distribución por número de columnas (desde C5):")
    for n, c in sorted(distrib.items()):
        print(f"   - {n} columnas: {c} hoja(s)")
    print(f"\n✅ Base seleccionada: {ncols_base} columnas")

    grupo_base  = [(h, df) for (h, df, n) in info if n == ncols_base]
    grupo_otras = [(h, df) for (h, df, n) in info if n != ncols_base]

    # Define la hoja y columnas de referencia
    hoja_ref, df_ref = grupo_base[0]
    columnas_base = list(df_ref.columns)
    print(f"🧭 Hoja de referencia: {hoja_ref}")

    # Normaliza el grupo base por posición
    dfs_base_norm = []
    for h, df in grupo_base:
        d = renombrar_por_posicion(df, columnas_base)
        d["HOJA_ORIGEN"] = h
        dfs_base_norm.append(d)

    # Identifica columnas adicionales (en otras hojas)
    first_seen = {}
    for idx, (h, df) in enumerate(grupo_otras):
        for j, col in enumerate(df.columns):
            if col not in columnas_base and col not in first_seen:
                first_seen[col] = (idx, j)
    extras_ordenadas = sorted(first_seen.keys(), key=lambda c: first_seen[c])

    # Define el orden final de columnas
    columnas_finales = columnas_base + extras_ordenadas

    # Alinea todas las hojas al esquema final
    dfs_final = []
    for h, df in grupo_base + grupo_otras:
        d = df.copy()
        for col in columnas_finales:
            if col not in d.columns:
                d[col] = pd.NA
        d = d[columnas_finales]
        d["HOJA_ORIGEN"] = h
        dfs_final.append(d)

    # Consolida todos los DataFrames
    consolidado = pd.concat(dfs_final, ignore_index=True)

    # Filtros y limpieza final
    consolidado = _limpiar_celdas_todas(consolidado) # Limpieza inicial de texto
    consolidado = filtrar_sin_dni(consolidado)
    consolidado = eliminar_filas_con_encabezados(consolidado)

    # <--- INICIO: PASO DE UNIFORMIZACIÓN ---
    print("\n🔧 Aplicando uniformización de datos (GRADO, SEXO, CENTRO, CONDICION ACTUAL)...")

    # Mapeo de GRADO
    if 'GRADO' in consolidado.columns:
        mapa_grado = {
            '00 INICIAL': 'INICIAL',
            '1ER GRADO': 'PRIMERO',
            '2DO GRADO': 'SEGUNDO',
            '3ER GRADO': 'TERCERO',
            '4TO GRADO': 'CUARTO',
            '5TO GRADO': 'QUINTO',
            '6TO GRADO': 'SEXTO'
        }
        series_limpia_grado = consolidado['GRADO'].astype(str).apply(_norm_text)
        consolidado['GRADO'] = series_limpia_grado.replace(mapa_grado)
        print("   - 'GRADO' uniformizado.")

    # Mapeo de SEXO
    if 'SEXO' in consolidado.columns:
        mapa_sexo = {
            'M': 'MASCULINO',
            'HOMBRE': 'MASCULINO',
            'F': 'FEMENINO',
            'MUJER': 'FEMENINO'
        }
        series_limpia_sexo = consolidado['SEXO'].astype(str).apply(_norm_text)
        consolidado['SEXO'] = series_limpia_sexo.map(mapa_sexo).fillna(series_limpia_sexo)
        print("   - 'SEXO' uniformizado.")

    # Mapeo de CENTRO
    if 'CENTRO' in consolidado.columns:
        consolidado['CENTRO'] = consolidado['CENTRO'].astype(str).apply(_norm_text)
        print("   - 'CENTRO' uniformizado (mayúsculas, sin tildes).")

    # Mapeo de CONDICION ACTUAL
    col_condicion = _find_col(consolidado, ["CONDICION ACTUAL", "CONDICIÓN ACTUAL"])

    if col_condicion:
        series_limpia = (
            consolidado[col_condicion]
            .astype(str)
            .apply(_norm_text)
            .replace('NAN', np.nan)
        )
        mask_activo = series_limpia.str.contains("ACTIVO", na=False)
        mask_retirado = series_limpia.str.contains("RETIRADO", na=False)

        consolidado[col_condicion] = np.nan
        consolidado.loc[mask_activo, col_condicion] = 'ACTIVO'
        consolidado.loc[mask_retirado, col_condicion] = 'INACTIVO'

        print("   - 'CONDICION ACTUAL' uniformizada (Retirados -> INACTIVO).")
    else:
        print("⚠️ No se encontró la columna 'CONDICIÓN ACTUAL'.")

    # --- NUEVO: Transformación de FECHA DE REGISTRO ---
    col_fecha_reg = _find_col(consolidado, ["FECHA DE REGISTRO", "FECHA REGISTRO"])

    if col_fecha_reg:
        # 1. Aplicar la función de transformación (devuelve objetos datetime o NaT)
        datetime_series = consolidado[col_fecha_reg].apply(transformar_fecha_registro)

        # 2. Formatear a string dd/mm/YYYY
        string_series = datetime_series.dt.strftime('%d/%m/%Y')

        # 3. Reemplazar 'NaT' (que es un string) por un nulo real
        consolidado[col_fecha_reg] = string_series.replace('NaT', np.nan)

        print(f"   - '{col_fecha_reg}' transformada y formateada (dd/mm/YYYY).")
    else:
        print("⚠️ No se encontró la columna 'FECHA DE REGISTRO'.")
    # --- FIN NUEVO ---

    # <--- FIN: PASO DE UNIFORMIZACIÓN ---

    # <--- INICIO: PASO DE REPORTE DE CALIDAD ---
    print("\n📊 Generando reporte de calidad de datos (Codebook)...")

    df_calidad = codebook(consolidado)

    try:
        CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)
        df_calidad.to_excel(SALIDA_CALIDAD, index=False, engine="openpyxl")
        print(f"✅ Reporte de calidad guardado en:\n{SALIDA_CALIDAD}")
    except Exception as e:
        print(f"[ERROR] No se pudo guardar el reporte de calidad: {e}")
    # <--- FIN: PASO DE REPORTE DE CALIDAD ---

    # Exportación final
    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)
    consolidado.to_csv(SALIDA, index=False, encoding="utf-8-sig", sep=',')

    print(f"\n✅ Consolidado final guardado en:\n{SALIDA}")
    print(f"   Filas totales: {len(consolidado):,}")
    print(f"   Columnas: {len(consolidado.columns)}")
    print(f"   Columnas base: {len(columnas_base)} | Extras añadidas: {len(extras_ordenadas)}")

# === EJECUCIÓN ===============================================================
if __name__ == "__main__":
    main()
