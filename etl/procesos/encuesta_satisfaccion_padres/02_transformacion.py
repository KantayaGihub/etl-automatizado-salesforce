

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de Limpieza, Deduplicación y Reporte de Calidad (Encuesta de Satisfacción).

Este script toma múltiples archivos .csv de "Encuesta de Satisfacción"
desde una carpeta, los combina, realiza una limpieza de datos
(incluyendo normalización de texto y DNI), y deduplica los registros
basándose en el DNI del Apoderado ('N Documento Apoderado').

ACTUALIZACIONES:
1. Renombra 'Documento de identidad' -> 'Documento de identidad Apoderado'.
2. Renombra 'Documento de identidad ' -> 'Documento de identidad Niño'.
3. Renombra 'N° de documento' -> 'N Documento Apoderado'.
4. Renombra 'N° de documento ' -> 'N Documento Niño'.
5. Convierte 'Marca temporal' a formato fecha (sin hora) en el archivo final.

REGLA: Conserva únicamente el registro más reciente según la 'Marca temporal'.

Genera dos archivos de salida:
1. Un .csv con los datos limpios y deduplicados ('Encuesta_Padres_Deduplicado.csv').
2. Un .xlsx con el reporte de calidad de los datos finales ('Encuesta_Padres_Deduplicado_CALIDAD.xlsx').

Librerías necesarias: pandas, numpy, openpyxl, glob
(Instalar con: pip install pandas numpy openpyxl)
"""

import pandas as pd
import numpy as np
import os # Librería para interactuar con el sistema operativo (crear carpetas y unir rutas)
import glob # <-- Para encontrar todos los archivos en la carpeta
import re # Para limpieza de texto
from datetime import datetime # Para fechas
import unicodedata # Para quitar tildes

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
        "Porcentaje Nulos (%": ((df.isnull().sum() / len(df)) * 100),
        "Valores únicos (#)": df.nunique(),
    })

    print("   [Calidad] Calculando min/max numéricos...")
    resumen["Mínimo"] = df.apply(lambda x: x.min(skipna=True) if pd.api.types.is_numeric_dtype(x) else None)
    resumen["Máximo"] = df.apply(lambda x: x.max(skipna=True) if pd.api.types.is_numeric_dtype(x) else None)
    
    print(f"   [Calidad] Verificando duplicados (PK={pk_col})...")
    resumen["Duplicados (Valores)"] = "No" # Default
    
    if pk_col in df.columns:
        total_dups_dni = df[pk_col].dropna().duplicated().sum()
        if total_dups_dni > 0:
            print(f"   -> ALERTA PK ({pk_col}): {total_dups_dni} duplicados encontrados.")
            resumen.loc[pk_col, "Duplicados (Valores)"] = f"¡SÍ! ({total_dups_dni} duplicados)"
        else:
            print(f"   -> Verificación PK ({pk_col}): OK (0 duplicados).")
            resumen.loc[pk_col, "Duplicados (Valores)"] = "No (PK Válida)"
            
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

def limpiar_y_deduplicar_encuesta_padre(input_folder, output_filename_csv, output_filename_excel):
    """
    CONSULTORA: Script de Limpieza y Deduplicación para "Encuesta de Satisfacción". (Versión 5.3)
    
    REGLA DE DEDUPLICACIÓN:
    La PK es el DNI del Apoderado ('N Documento Apoderado').
    Conserva ÚNICAMENTE el registro más reciente según la 'Marca temporal'.
    """
    
    print(f"--- Iniciando Proceso de Limpieza y Deduplicación para la carpeta: {input_folder} ---")
    
    try:
        # --- PASO 1: Carga y Combinación de Múltiples Archivos ---
        
        search_pattern = os.path.join(input_folder, "*.csv")
        file_list = glob.glob(search_pattern)
        
        if not file_list:
            print(f"--- ERROR CRÍTICO ---")
            print(f"No se encontraron archivos .csv en la carpeta: {input_folder}")
            return

        print(f"Se encontraron {len(file_list)} archivos. Cargando...")
        
        df_list = []
        for file_path in file_list:
            print(f" - Cargando: {os.path.basename(file_path)}")
            try:
                df_temp = pd.read_csv(file_path)
                df_list.append(df_temp)
            except Exception as e:
                 print(f"   Error al cargar {os.path.basename(file_path)}: {e}. Omitiendo archivo.")

        if not df_list:
             print(f"--- ERROR CRÍTICO ---")
             print(f"No se pudo cargar ningún archivo CSV válido.")
             return
            
        df = pd.concat(df_list, ignore_index=True)
        filas_originales = len(df)
        print(f"Archivos combinados. Filas totales: {filas_originales}, Columnas: {len(df.columns)}")
        
    except FileNotFoundError:
        print(f"--- ERROR CRÍTICO ---")
        print(f"No se pudo encontrar la ruta: {file_path}")
        return
    except Exception as e:
        print(f"Error inesperado al leer los archivos: {e}")
        return

    # --- PASO 1.5: Renombrar Columnas (Solicitud Usuario) ---
    print("Renombrando columnas clave...")
    rename_map = {
        'Documento de identidad': 'Documento de identidad Apoderado', # Sin espacio
        'Documento de identidad ': 'Documento de identidad Niño',  # Con espacio
        'N° de documento': 'N Documento Apoderado', # Sin espacio
        'N° de documento ': 'N Documento Niño' # Con espacio
    }
    df = df.rename(columns=rename_map)
    
    # --- Definición de Columnas (Mapeo) ---
    # !!! Se usan los NUEVOS nombres de columnas para el resto del script
    col_num_doc_apod = 'N Documento Apoderado' # (NUESTRA PK)
    col_num_doc_nino = 'N Documento Niño'
    
    # --- PASO 2: Limpieza Previa Obligatoria ---
    print("Realizando limpieza previa de campos clave...")

    # [A] Limpiar DNI Apoderado (PK)
    if col_num_doc_apod in df.columns:
        df[col_num_doc_apod] = (
            df[col_num_doc_apod]
            .astype(str)
            .str.replace(r'\\.0$', '', regex=True)
            .str.replace(r'\\D', '', regex=True)
        )
        df[col_num_doc_apod] = df[col_num_doc_apod].replace(['nan', '0', ''], np.nan)

    # [B] Limpiar DNI Niño (Campo secundario)
    if col_num_doc_nino in df.columns:
        df[col_num_doc_nino] = (
            df[col_num_doc_nino]
            .astype(str)
            .str.replace(r'\\.0$', '', regex=True)
            .str.replace(r'\\D', '', regex=True)
        )
        df[col_num_doc_nino] = df[col_num_doc_nino].replace(['nan', '0', ''], np.nan)

    # [C] Convertir Marca Temporal (para ordenar)
    if 'Marca temporal' in df.columns:
        # 'dayfirst=True' asume formato D/M/A (común en encuestas de Latam)
        df['Marca temporal'] = pd.to_datetime(df['Marca temporal'], dayfirst=True, errors='coerce')

    # [D] Limpiar TODOS los campos de texto (object)
    print("Limpiando campos de texto...")
    for col in df.select_dtypes(include=['object']).columns:
        if col not in [col_num_doc_apod, col_num_doc_nino]: # Evitar doble limpieza
            df[col] = (
                df[col]
                .astype(str)
                .str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('ascii') # Quitar tildes
                .str.replace(r"\\s+", " ", regex=True) # Colapsar espacios
                .str.replace(r"[–—]", "-", regex=True) # Normalizar guiones
                .str.upper()
                .str.strip()
                .replace('NAN', np.nan)
                .replace('', np.nan)
            )
        
    # --- PASO 3: Identificar Grupos de Datos (Únicos vs Duplicados vs Nulos) ---
    
    subset_key = [col_num_doc_apod]
    
    mask_duplicados = (
        df.duplicated(subset=subset_key, keep=False) & 
        df[col_num_doc_apod].notnull()
    )
    
    df_duplicados_raw = df[mask_duplicados].copy()
    df_unicos_y_nulos = df[~mask_duplicados].copy()

    print(f"Registros únicos y nulos (se conservan): {len(df_unicos_y_nulos)}")
    print(f"Registros con DNI Apoderado duplicado (a procesar): {len(df_duplicados_raw)}")

    # --- PASO 4: Resolver Duplicados (Conservar el último registro) ---
    
    df_resueltos = pd.DataFrame(columns=df.columns)
    
    if not df_duplicados_raw.empty:
        print("Procesando duplicados (ordenando por 'Marca temporal')...")
        
        df_duplicados_ordenados = df_duplicados_raw.sort_values(by='Marca temporal', ascending=True, na_position='first')
        df_resueltos = df_duplicados_ordenados.drop_duplicates(subset=subset_key, keep='last')
        
        print(f"Registros conservados tras el proceso de deduplicación: {len(df_resueltos)}")

    # --- PASO 5: Reconstruir y Guardar el DataFrame ---
    
    df_final = pd.concat([df_unicos_y_nulos, df_resueltos], ignore_index=True)
    df_final = df_final.sort_index()
    filas_finales = len(df_final)

    # --- (Solicitud Usuario): Convertir Marca temporal a solo fecha ---
    if 'Marca temporal' in df_final.columns:
        df_final['Marca temporal'] = df_final['Marca temporal'].dt.date

    print("Proceso de deduplicación completado.")

    # --- PASO 6: Guardado del Archivo Limpio (CSV) ---
    try:
        output_dir = os.path.dirname(output_filename_csv)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Se creó el directorio de salida: {output_dir}")
            
        df_final.to_csv(output_filename_csv, index=False, encoding='utf-8-sig', sep=',')
        
        print(f"\n--- ¡PROCESO DE DEDUPLICACIÓN COMPLETADO! ---")
        print(f"Filas originales: {filas_originales}")
        print(f"Filas eliminadas: {filas_originales - filas_finales}")
        print(f"Filas finales (guardadas): {filas_finales}")
        print(f"Archivo limpio guardado en: {output_filename_csv}")
        
    except PermissionError:
        print(f"\n--- ERROR AL GUARDAR (CSV) ---")
        print(f"Permiso denegado al intentar guardar en: {output_filename_csv}")
        print("Acción Requerida: Cierre el archivo si lo tiene abierto en Excel e intente de nuevo.")
    except Exception as e:
        print(f"\n--- ERROR AL GUARDAR (CSV) ---")
        print(f"Error inesperado al guardar el archivo: {e}")

    # --- PASO 7: Guardado del Reporte de Calidad (Excel) ---
    print("\n📊 Generando reporte de calidad de datos (Codebook)...")
    
    # Se usa el NUEVO nombre de columna 'N Documento Apoderado' como PK
    df_calidad = codebook(df_final, pk_col=col_num_doc_apod)
    
    try:
        df_calidad.to_excel(output_filename_excel, index=False, engine="openpyxl")
        print(f"✅ Reporte de calidad guardado en:\n{output_filename_excel}")
    except PermissionError:
        print(f"\n--- ERROR AL GUARDAR (Excel Calidad) ---")
        print(f"Permiso denegado al intentar guardar en: {output_filename_excel}")
        print("Acción Requerida: Cierre el archivo si lo tiene abierto en Excel e intente de nuevo.")
    except Exception as e:
        print(f"[ERROR] No se pudo guardar el reporte de calidad: {e}")


# --- EJECUCIÓN DE LA LIMPIEZA Y DEDUPLICACIÓN ---
if __name__ == "__main__":

    # 1. Ruta de ENTRADA donde el ETL descargó los EXCEL
    input_folder_excel = "entrada"

    # 2. CARPETA TEMPORAL donde convertiremos Excel → CSV
    input_folder_encuesta = "entrada/csv"
    os.makedirs(input_folder_encuesta, exist_ok=True)

    print("\n=== Convirtiendo SOLO archivos de ENCUESTA Excel (.xlsx) a CSV ===")

    # Lista exacta de archivos válidos
    VALID_FILES = {
        "Encuesta_Inicial_1y2.xlsx",
        "Encuesta_3y4.xlsx",
        "Encuesta_5y6.xlsx"
    }

    # Convertir **solo** los Excel de encuesta a CSV
    for fname in os.listdir(input_folder_excel):
        if fname in VALID_FILES:
            excel_path = os.path.join(input_folder_excel, fname)
            csv_name = fname.replace(".xlsx", ".csv")
            csv_path = os.path.join(input_folder_encuesta, csv_name)

            print(f"→ {fname}  →  {csv_name}")

            df_temp = pd.read_excel(excel_path)
            df_temp.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print("✓ Conversión completa. Usando los CSV filtrados para la limpieza.\n")

    # 3. Rutas de salida
    output_folder = "salidas"
    os.makedirs(output_folder, exist_ok=True)

    output_filename_csv = "Encuesta_Padres_Deduplicado.csv"
    output_filename_excel = "Encuesta_Padres_Deduplicado_CALIDAD.xlsx"

    output_path_csv = os.path.join(output_folder, output_filename_csv)
    output_path_excel = os.path.join(output_folder, output_filename_excel)

    # 4. Ejecuta la función ORIGINAL (SIN CAMBIAR NADA)
    limpiar_y_deduplicar_encuesta_padre(
        input_folder_encuesta,
        output_path_csv,
        output_path_excel
    )
