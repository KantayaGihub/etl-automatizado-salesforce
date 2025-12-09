import pandas as pd
import numpy as np
import os
import re
from pathlib import Path
import unicodedata 
from datetime import datetime 
import csv # Requerido para forzar las comillas

# --- MÓDULO DE CALIDAD (CODEBOOK) ---
def codebook(df, pk_col):
    """
    Genera un DataFrame de resumen (codebook) sobre la calidad de datos
    del DataFrame final.
    """
    print("   [Calidad] Calculando tipos, nulos y únicos...")
    resumen = pd.DataFrame({
        "Tipo": df.dtypes,
        "Nulos (#)": df.isnull().sum(),
        "Porcentaje Nulos (%)": ((df.isnull().sum() / len(df)) * 100),
        "Valores únicos (#)": df.nunique(),
    })

    print("   [Calidad] Calculando min/max numéricos...")
    resumen["Mínimo"] = df.apply(lambda x: x.min(skipna=True) if pd.api.types.is_numeric_dtype(x) else None)
    resumen["Máximo"] = df.apply(lambda x: x.max(skipna=True) if pd.api.types.is_numeric_dtype(x) else None)
    
    print(f"   [Calidad] Verificando duplicados (PK={pk_col})...")
    resumen["Duplicados (Valores)"] = "No" # Default
    
    if pk_col in df.columns:
        total_dups_pk = df[pk_col].dropna().duplicated().sum()
        if total_dups_pk > 0:
            print(f"   -> ALERTA PK ({pk_col}): {total_dups_pk} duplicados encontrados.")
            resumen.loc[pk_col, "Duplicados (Valores)"] = f"¡SÍ! ({total_dups_pk} duplicados)"
        else:
            print(f"   -> Verificación PK ({pk_col}): OK (0 duplicados).")
            resumen.loc[pk_col, "Duplicados (Valores)"] = "No (PK Válida)"
            
    # Chequear duplicados para otras columnas
    for col in df.columns:
        if col != pk_col and df[col].duplicated().any():
            resumen.loc[col, "Duplicados (Valores)"] = "Sí"

    print("   [Calidad] Extrayendo muestra de valores únicos (límite 50)...")
    def get_unique_values(x):
        unicos = x.dropna().unique()
        if x.nunique() > 50:
            return f"({x.nunique()} valores) Ej: {str(list(unicos[:5]))}"
        else:
            return str(list(unicos))
            
    resumen["Valores únicos (Muestra)"] = df.apply(get_unique_values)
    
    resumen = resumen.reset_index().rename(columns={"index": "Variable"})
    return resumen
# --- FIN MÓDULO DE CALIDAD ---


# --- INICIO FUNCIONES DE PROCESAMIENTO ---

def get_metadata_cols(df_columns):
    """
    Identifica las columnas de metadatos basándose en la columna 'ÁREA'
    y las columnas finales ('TOTAL PREGUNTAS...', 'PORCENTAJE...', 'NIVEL...').
    """
    try:
        col_iniciales_base = list(df_columns[:df_columns.get_loc("ÁREA") + 1])
    except KeyError:
        print("      -> ERROR FATAL: No se encontró la columna 'ÁREA'. Saltando hoja.")
        return None

    cols_finales_score = ["TOTAL PREGUNTAS ACERTADAS", "PORCENTAJE DE PROMEDIO", "NIVEL DE LOGRO"]
    
    for col in cols_finales_score:
        if col not in df_columns:
            print(f"      -> ADVERTENCIA: No se encontró la columna de metadatos '{col}'.")
            
    col_iniciales = col_iniciales_base + [c for c in cols_finales_score if c in df_columns]
    
    return col_iniciales


def procesar_hoja_estandar(df, nombre_area):
    """
    Procesa hojas con el patrón de 'COMPETENCIA X NIVEL DE LOGRO...'
    (Usado para Matemática, Comunicación)
    """
    print(f"      -> Procesando {nombre_area} (Lógica: Estándar)...")
    
    # 1. Limpiar columnas
    df2 = df.loc[:, ~df.columns.str.match(r'^P\d+$')]
    df2.columns = df2.columns.str.replace(r'[\n\r\t;]+', ' ', regex=True)
    df2.columns = df2.columns.str.replace(r'\s+', ' ', regex=True).str.strip()
    df2.columns = df2.columns.str.upper() # Convertir a Mayúsculas
    df2 = df2.loc[:, ~df2.columns.duplicated()]

    # 2. Selección de columnas “iniciales” (metadatos)
    col_iniciales = get_metadata_cols(df2.columns)
    if col_iniciales is None:
        return None
    df_inicial = df2[col_iniciales]

    # 3. Selección de columnas restantes (competencias) + 'DNI'
    col_restantes = [col for col in df2.columns if col not in col_iniciales]
    df_restante = df2[['DNI'] + [col for col in col_restantes if col != 'DNI']]

    # 4. Pivote de Promedios (df_restante_a)
    cols_promedio = [c for c in df_restante.columns if c.startswith("PROMEDIO COMPETENCIA")]
    if not cols_promedio:
        print(f"      -> ERROR: No se encontraron columnas de 'PROMEDIO COMPETENCIA' para {nombre_area}. Saltando hoja.")
        return None
        
    df_restante_a = df_restante.melt(
        id_vars="DNI", 
        value_vars=cols_promedio, 
        var_name="Competencia_temp", 
        value_name="Valor_Promedio"
    )
    df_restante_a['Competencia_num'] = df_restante_a['Competencia_temp'].str.extract(r'(COMPETENCIA \d+)', expand=False)
    df_restante_a['Competencia_texto'] = df_restante_a['Competencia_temp'].str.extract(r'COMPETENCIA \d+\s*(.*)', expand=False)
    df_restante_a['Competencia_texto'] = df_restante_a['Competencia_texto'].str.replace(':', '', regex=False).str.strip()
    df_restante_a = df_restante_a[["DNI", "Competencia_num", "Competencia_texto", "Valor_Promedio"]]

    # 5. Pivote de Valores Alcanzados (df_restante_b)
    valor_alcanzado_regex = r'COMPETENCIA \d+ NIVEL DE LOGRO.*'
    cols_no_prom = [c for c in df_restante.columns if re.match(valor_alcanzado_regex, c)]
    
    if not cols_no_prom:
        print(f"      -> ERROR: No se encontraron columnas de 'Valor Alcanzado' (Patrón: {valor_alcanzado_regex}) para {nombre_area}. Saltando hoja.")
        return None

    df_restante_b = df_restante.melt(
        id_vars=["DNI"],
        value_vars=cols_no_prom,
        var_name="Competencia_temp",
        value_name="Valor_alcanzado"
    )
    df_restante_b['Competencia_num_merge'] = df_restante_b['Competencia_temp'].str.extract(r'(COMPETENCIA \d+)', expand=False)

    # 6. Merge A + B
    df_restante_c = pd.merge(df_restante_a, df_restante_b, left_on=["DNI", "Competencia_num"], right_on=["DNI", "Competencia_num_merge"], how = "inner")

    # 7. Merge C + Metadatos
    df_final = pd.merge(
        df_restante_c,
        df_inicial,
        on="DNI",
        how="left"
    )

    # 8. Limpieza final y añadir Área
    df_final.drop(columns=["Competencia_temp_x", "Competencia_temp_y", "Competencia_num_merge"], inplace=True, errors='ignore')
    df_final.rename(columns={"Competencia_num": "Competencia"}, inplace=True)
    
    return df_final


def procesar_hoja_socio(df, nombre_area):
    """
    Procesa hojas con el patrón de 'NIVEL DE LOGRO COMPETENCIA X...'
    (Usado para Socioemocional)
    """
    print(f"      -> Procesando {nombre_area} (Lógica: Socioemocional)...")
    
    # 1. Limpiar columnas
    df2 = df.loc[:, ~df.columns.str.match(r'^P\d+$')]
    df2.columns = df2.columns.str.replace(r'[\n\r\t;]+', ' ', regex=True)
    df2.columns = df2.columns.str.replace(r'\s+', ' ', regex=True).str.strip()
    df2.columns = df2.columns.str.upper() # Convertir a Mayúsculas
    df2 = df2.loc[:, ~df2.columns.duplicated()]

    # 2. Selección de columnas “iniciales” (metadatos)
    col_iniciales = get_metadata_cols(df2.columns)
    if col_iniciales is None:
        return None
    df_inicial = df2[col_iniciales]

    # 3. Selección de columnas restantes (competencias) + 'DNI'
    col_restantes = [col for col in df2.columns if col not in col_iniciales]
    df_restante = df2[['DNI'] + [col for col in col_restantes if col != 'DNI']]

    # 4. Pivote de Promedios (df_restante_a)
    cols_promedio = [c for c in df_restante.columns if c.startswith("PROMEDIO COMPETENCIA")]
    if not cols_promedio:
        print(f"      -> ERROR: No se encontraron columnas de 'PROMEDIO COMPETENCIA' para {nombre_area}. Saltando hoja.")
        return None
        
    df_restante_a = df_restante.melt(
        id_vars="DNI", 
        value_vars=cols_promedio, 
        var_name="Competencia_temp", 
        value_name="Valor_Promedio"
    )
    df_restante_a['Competencia_num'] = df_restante_a['Competencia_temp'].str.extract(r'(COMPETENCIA \d+)', expand=False)
    df_restante_a['Competencia_texto'] = df_restante_a['Competencia_temp'].str.extract(r'COMPETENCIA \d+\s*(.*)', expand=False)
    df_restante_a['Competencia_texto'] = df_restante_a['Competencia_texto'].str.replace(':', '', regex=False).str.strip()
    df_restante_a = df_restante_a[["DNI", "Competencia_num", "Competencia_texto", "Valor_Promedio"]]

    # 5. Pivote de Valores Alcanzados (df_restante_b)
    valor_alcanzado_regex = r'NIVEL DE LOGRO COMPETENCIA \d+.*' # <-- PATRÓN DIFERENTE
    cols_no_prom = [c for c in df_restante.columns if re.match(valor_alcanzado_regex, c)]
    
    if not cols_no_prom:
        print(f"      -> ERROR: No se encontraron columnas de 'Valor Alcanzado' (Patrón: {valor_alcanzado_regex}) para {nombre_area}. Saltando hoja.")
        return None

    df_restante_b = df_restante.melt(
        id_vars=["DNI"],
        value_vars=cols_no_prom,
        var_name="Competencia_temp",
        value_name="Valor_alcanzado"
    )
    df_restante_b['Competencia_num_merge'] = df_restante_b['Competencia_temp'].str.extract(r'(COMPETENCIA \d+)', expand=False)

    # 6. Merge A + B
    df_restante_c = pd.merge(df_restante_a, df_restante_b, left_on=["DNI", "Competencia_num"], right_on=["DNI", "Competencia_num_merge"], how = "inner")

    # 7. Merge C + Metadatos
    df_final = pd.merge(
        df_restante_c,
        df_inicial,
        on="DNI",
        how="left"
    )

    # 8. Limpieza final y añadir Área
    df_final.drop(columns=["Competencia_temp_x", "Competencia_temp_y", "Competencia_num_merge"], inplace=True, errors='ignore')
    df_final.rename(columns={"Competencia_num": "Competencia"}, inplace=True)
    
    return df_final
    

def procesar_hoja_tecnologia(df, nombre_area):
    """
    Procesa la hoja de Tecnologia (patrón 'Promedio C1...').
    """
    print(f"      -> Procesando {nombre_area} (Lógica: Tecnologia)...")
    
    # 1. Limpiar columnas
    df2 = df.loc[:, ~df.columns.str.match(r'^P\d+$')]
    df2.columns = df2.columns.str.replace(r'[\n\r\t;]+', ' ', regex=True)
    df2.columns = df2.columns.str.replace(r'\s+', ' ', regex=True).str.strip()
    df2.columns = df2.columns.str.upper() # Convertir a Mayúsculas
    df2 = df2.loc[:, ~df2.columns.duplicated()]

    # 2. Selección de columnas “iniciales” (metadatos)
    col_iniciales = get_metadata_cols(df2.columns)
    if col_iniciales is None:
        return None
    df_inicial = df2[col_iniciales]

    # 3. Selección de columnas restantes (competencias) + 'DNI'
    col_restantes = [col for col in df2.columns if col not in col_iniciales]
    df_restante = df2[['DNI'] + [col for col in col_restantes if col != 'DNI']]

    # 4. Pivote de Promedios (df_restante_a)
    regex_promedio = r'PROMEDIO C\d+.*'
    cols_promedio = [c for c in df_restante.columns if re.match(regex_promedio, c)]
    if not cols_promedio:
        print(f"      -> ERROR: No se encontraron columnas de 'Promedio' (Patrón: {regex_promedio}) para {nombre_area}. Saltando hoja.")
        return None
        
    df_restante_a = df_restante.melt(
        id_vars="DNI", 
        value_vars=cols_promedio, 
        var_name="Competencia_temp", 
        value_name="Valor_Promedio"
    )
    # Extrae 'C1' y lo convierte a 'COMPETENCIA 1'
    df_restante_a['Competencia_num'] = df_restante_a['Competencia_temp'].str.extract(r'(C\d+)', expand=False)
    df_restante_a['Competencia_num'] = df_restante_a['Competencia_num'].str.replace('C', 'COMPETENCIA ')
    
    df_restante_a['Competencia_texto'] = df_restante_a['Competencia_temp'].str.extract(r'C\d+\s*(.*)', expand=False)
    df_restante_a['Competencia_texto'] = df_restante_a['Competencia_texto'].str.replace(':', '', regex=False).str.strip()
    df_restante_a = df_restante_a[["DNI", "Competencia_num", "Competencia_texto", "Valor_Promedio"]]

    # 5. Pivote de Valores Alcanzados (df_restante_b)
    valor_alcanzado_regex = r'COMPETENCIA \d+ NIVEL DE LOGRO.*'
    cols_no_prom = [c for c in df_restante.columns if re.match(valor_alcanzado_regex, c)]
    
    if not cols_no_prom:
        print(f"      -> ERROR: No se encontraron columnas de 'Valor Alcanzado' (Patrón: {valor_alcanzado_regex}) para {nombre_area}. Saltando hoja.")
        return None

    df_restante_b = df_restante.melt(
        id_vars=["DNI"],
        value_vars=cols_no_prom,
        var_name="Competencia_temp",
        value_name="Valor_alcanzado"
    )
    df_restante_b['Competencia_num_merge'] = df_restante_b['Competencia_temp'].str.extract(r'(COMPETENCIA \d+)', expand=False)

    # 6. Merge A + B
    df_restante_c = pd.merge(df_restante_a, df_restante_b, left_on=["DNI", "Competencia_num"], right_on=["DNI", "Competencia_num_merge"], how = "inner")

    # 7. Merge C + Metadatos
    df_final = pd.merge(
        df_restante_c,
        df_inicial,
        on="DNI",
        how="left"
    )

    # 8. Limpieza final y añadir Área
    df_final.drop(columns=["Competencia_temp_x", "Competencia_temp_y", "Competencia_num_merge"], inplace=True, errors='ignore')
    df_final.rename(columns={"Competencia_num": "Competencia"}, inplace=True)
    
    return df_final


def procesar_hoja_ciencias(df, nombre_area):
    """
    Procesa la hoja de Ciencias.
    CORREGIDO: Asigna el "PORCENTAJE DE PROMEDIO" (metadato) 
    como el "Valor_Promedio" (de competencia), ya que solo tiene una.
    """
    print(f"      -> Procesando {nombre_area} (Lógica: Ciencias - CORREGIDA)...")

    # 1. Limpiar columnas (Sin cambios)
    df2 = df.loc[:, ~df.columns.str.match(r'^P\d+$')]
    df2.columns = df2.columns.str.replace(r'[\n\r\t;]+', ' ', regex=True)
    df2.columns = df2.columns.str.replace(r'\s+', ' ', regex=True).str.strip()
    df2.columns = df2.columns.str.upper()
    df2 = df2.loc[:, ~df2.columns.duplicated()]

    # 2. Selección de columnas “iniciales” (metadatos) (Sin cambios)
    col_iniciales = get_metadata_cols(df2.columns)
    if col_iniciales is None:
        return None
    df_inicial = df2[col_iniciales]

    # 3. Selección de columnas restantes (competencias) + 'DNI' (Sin cambios)
    col_restantes = [col for col in df2.columns if col not in col_iniciales]
    df_restante = df2[['DNI'] + [col for col in col_restantes if col != 'DNI']]

    # 4. Pivote de Texto (df_texto) (Sin cambios)
    cols_texto = [c for c in df_restante.columns if re.match(r'COMPETENCIA \d+:.*', c)]
    if not cols_texto:
        print(f"      -> ERROR: No se encontraron columnas de 'Texto de Competencia' para {nombre_area}. Saltando hoja.")
        return None
        
    df_pivotado_texto = df_restante.melt(
        id_vars=["DNI"],
        value_vars=cols_texto,
        var_name="Competencia_temp",
        value_name="Competencia_texto"
    )
    df_pivotado_texto['Competencia'] = df_pivotado_texto['Competencia_temp'].str.extract(r'(COMPETENCIA \d+)', expand=False)
    df_pivotado_texto['Competencia_texto'] = df_pivotado_texto['Competencia_temp'].str.extract(r'COMPETENCIA \d+\s*(.*)', expand=False)
    df_pivotado_texto['Competencia_texto'] = df_pivotado_texto['Competencia_texto'].str.replace(':', '', regex=False).str.strip()

    # 5. Pivote de Valores (df_valor) (Sin cambios)
    cols_valor = [c for c in df_restante.columns if re.match(r'COMPETENCIA \d+ NIVEL DE LOGRO.*', c)]
    
    if not cols_valor:
        print(f"      -> ERROR: No se encontraron columnas de 'Valor Alcanzado' para {nombre_area}. Saltando hoja.")
        return None

    df_pivotado_valor = df_restante.melt(
        id_vars=["DNI"],
        value_vars=cols_valor,
        var_name="Competencia_temp",
        value_name="Valor_alcanzado"
    )
    df_pivotado_valor['Competencia'] = df_pivotado_valor['Competencia_temp'].str.extract(r'(COMPETENCIA \d+)', expand=False)
    
    # 6. Merge Texto + Valor (Sin cambios)
    df_pivotado = pd.merge(
        df_pivotado_texto[["DNI", "Competencia", "Competencia_texto"]],
        df_pivotado_valor[["DNI", "Competencia", "Valor_alcanzado"]],
        on=["DNI", "Competencia"],
        how="inner"
    )

    # 7. Merge Pivote + Metadatos (Sin cambios)
    # (Se eliminó el paso 7 anterior que añadía 'Valor_Promedio' como nulo)
    df_final = pd.merge(
        df_pivotado,
        df_inicial,
        on="DNI",
        how="left"
    )

    # 8. --- INICIO DE LA CORRECCIÓN ---
    # Asignar el promedio.
    # Como Ciencias solo tiene 1 competencia, su "Valor_Promedio" (de competencia)
    # es el mismo que el "PORCENTAJE DE PROMEDIO" (del área).
    if "PORCENTAJE DE PROMEDIO" in df_final.columns:
        df_final['Valor_Promedio'] = df_final['PORCENTAJE DE PROMEDIO']
    else:
        # Si la columna de metadatos no existiera por alguna razón, se llena con nulos
        print(f"      -> ADVERTENCIA: No se encontró 'PORCENTAJE DE PROMEDIO' para asignar a 'Valor_Promedio' en {nombre_area}.")
        df_final['Valor_Promedio'] = np.nan
    # --- FIN DE LA CORRECCIÓN ---

    # 9. Limpieza final (Sin cambios)
    df_final.drop(columns=["Competencia_temp_x", "Competencia_temp_y"], inplace=True, errors='ignore')
    
    return df_final

# --- FIN FUNCIONES DE PROCESAMIENTO ---


# --- INICIO FUNCIÓN DE LIMPIEZA DE DATOS ---
def limpiar_registros(df_raw):
    """
    Aplica trim y reemplaza tabs/newlines/semicolons en todas las columnas de texto.
    """
    print("         ... Limpiando (trimming) datos de texto...")
    
    # Selecciona columnas tipo 'object' (numpy) y 'string' (pandas)
    cols_texto = df_raw.select_dtypes(include=['object', 'string']).columns
    
    for col in cols_texto:
        # 1. Reemplaza saltos de línea (\n, \r), tabs (\t) y punto y coma (;) con un espacio
        df_raw[col] = df_raw[col].str.replace(r'[\n\r\t;]+', ' ', regex=True)
        # 2. Colapsa espacios múltiples (creados por el paso 1) en uno solo y aplica trim
        df_raw[col] = df_raw[col].str.replace(r'\s+', ' ', regex=True).str.strip()
            
    return df_raw
# --- FIN FUNCIÓN DE LIMPIEZA DE DATOS ---


# --- BLOQUE DE EJECUCIÓN PRINCIPAL ---
print("--- Iniciando Proceso de Consolidación de Áreas ---")

# 1. Ruta de ENTRADA (El Excel con las hojas PE_)
input_file_path = "entrada/PE25_Ventanilla_Resultados.xlsx"

# 2. Ruta de SALIDA (La CARPETA donde se guardarán los archivos)
output_folder_path = "salida/promedios_areas"

lista_dfs_finales = [] # Lista para guardar los DataFrames procesados

try:
    # 3. Leer el archivo Excel una sola vez
    print(f"   [Cargando] Leyendo archivo: {input_file_path}")
    xls = pd.ExcelFile(input_file_path)
    
    # 4. Procesar Hoja "Matemática"
    print(f"   [Procesando] Hoja: PE_Matemática...")
    try:
        df_raw_mate = pd.read_excel(xls, sheet_name="PE_Matemática", dtype={"DNI": str})
        df_raw_mate = limpiar_registros(df_raw_mate) 
        df_proc_mate = procesar_hoja_estandar(df_raw_mate, "Matemática")
        if df_proc_mate is not None:
            lista_dfs_finales.append(df_proc_mate)
        
    except Exception as e:
        print(f"      -> ERROR al procesar la hoja 'PE_Matemática': {e}")

    # 5. Procesar Hoja "Comunicación"
    print(f"   [Procesando] Hoja: PE_Comunicación...")
    try:
        df_raw_comu = pd.read_excel(xls, sheet_name="PE_Comunicación", dtype={"DNI": str})
        df_raw_comu = limpiar_registros(df_raw_comu) 
        df_proc_comu = procesar_hoja_estandar(df_raw_comu, "Comunicación")
        if df_proc_comu is not None:
            lista_dfs_finales.append(df_proc_comu)
        
    except Exception as e:
        print(f"      -> ERROR al procesar la hoja 'PE_Comunicación': {e}")

    # 6. Procesar Hoja "Socioemocional"
    print(f"   [Procesando] Hoja: PE_Socioemocional...")
    try:
        df_raw_socio = pd.read_excel(xls, sheet_name="PE_Socioemocional", dtype={"DNI": str})
        df_raw_socio = limpiar_registros(df_raw_socio) 
        df_proc_socio = procesar_hoja_socio(df_raw_socio, "Socioemocional")
        if df_proc_socio is not None:
            lista_dfs_finales.append(df_proc_socio)
        
    except Exception as e:
        print(f"      -> ERROR al procesar la hoja 'PE_Socioemocional': {e}")

    # 7. Procesar Hoja "Tecnologia" (AÑADIDA)
    print(f"   [Procesando] Hoja: PE_Tecnologia...")
    try:
        df_raw_tec = pd.read_excel(xls, sheet_name="PE_Tecnologia", dtype={"DNI": str})
        df_raw_tec = limpiar_registros(df_raw_tec) 
        df_proc_tec = procesar_hoja_tecnologia(df_raw_tec, "Tecnologia")
        if df_proc_tec is not None:
            lista_dfs_finales.append(df_proc_tec)
        
    except Exception as e:
        print(f"      -> ERROR al procesar la hoja 'PE_Tecnologia': {e}")

    # 8. Procesar Hoja "Ciencias" (AÑADIDA)
    print(f"   [Procesando] Hoja: PE_Ciencias...")
    try:
        df_raw_ciencias = pd.read_excel(xls, sheet_name="PE_Ciencias", dtype={"DNI": str})
        df_raw_ciencias = limpiar_registros(df_raw_ciencias) # <-- Aplicar limpieza de datos
        df_proc_ciencias = procesar_hoja_ciencias(df_raw_ciencias, "Ciencias")
        if df_proc_ciencias is not None:
            lista_dfs_finales.append(df_proc_ciencias)
    except Exception as e:
        print(f"      -> ERROR al procesar la hoja 'PE_Ciencias': {e}")


except FileNotFoundError:
    print(f"--- ERROR CRÍTICO ---")
    print(f"Archivo no encontrado en la ruta: {input_file_path}")
    exit()
except Exception as e:
    print(f"--- ERROR CRÍTICO ---")
    print(f"Error al leer el archivo Excel: {e}")
    exit()

# 9. Guardar (si se procesó algo)
if not lista_dfs_finales:
    print("--- PROCESO FALLIDO ---")
    print("No se pudo procesar ninguna hoja. Revisa los errores.")
else:
    print("\n   [Concatenando] Uniendo todas las áreas...")
    BD_final = pd.concat(lista_dfs_finales, ignore_index=True)

    print("\n   [Guardando] Creando archivos de salida...")
    
    # 10. Definir rutas de salida y crear carpeta
    output_folder = Path(output_folder_path)
    output_folder.mkdir(parents=True, exist_ok=True)
    
    output_csv = output_folder / "BD_Promedios_Areas_Consolidado.csv"
    output_excel_calidad = output_folder / "BD_Promedios_Areas_CALIDAD.xlsx"
    
    try:
        # 11. Guardar CSV final
        BD_final.to_csv(output_csv, 
                        index=False, 
                        encoding='utf-8-sig', 
                        sep=',', # <-- Separador Coma
                        quoting=csv.QUOTE_ALL) # <-- Forzando comillas
        
        print(f"\n--- ¡PROCESO COMPLETADO! ---")
        print(f"Archivo consolidado guardado en: {output_csv}")
        
        # 12. Generar y Guardar Codebook
        print("\nGenerando Codebook de calidad...")
        df_calidad = codebook(BD_final, pk_col='DNI') 
        df_calidad.to_excel(output_excel_calidad, index=False, engine="openpyxl")
        print(f"Reporte de Calidad guardado en: {output_excel_calidad}")

    except PermissionError:
        print(f"\n--- ERROR AL GUARDAR ---")
        print("Permiso denegado. Cierre los archivos CSV o Excel si los tiene abiertos e intente de nuevo.")
    except Exception as e:
        print(f"\n--- ERROR AL GUARDAR ---")
        print(f"Error inesperado al guardar los archivos: {e}")
