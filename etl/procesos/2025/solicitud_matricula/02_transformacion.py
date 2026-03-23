#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de Limpieza, Deduplicación y Reporte de Calidad.

Este script toma un archivo Excel de "Solicitud de Matrícula",
realiza una limpieza de datos, deduplica los registros de estudiantes
basándose en su DNI (conservando solo el más reciente),
y genera dos archivos de salida:

1. Un .csv con los datos limpios y deduplicados ('Sol_Mtr_Deduplicado.csv').
2. Un .xlsx con el reporte de calidad de los datos finales ('Sol_Mtr_Deduplicado_CALIDAD.xlsx').

Librerías necesarias: pandas, numpy, openpyxl
(Instalar con: pip install pandas numpy openpyxl)
"""

import pandas as pd
import numpy as np
import os # Librería para interactuar con el sistema operativo (crear carpetas y unir rutas)
import re # Para limpieza de texto
from datetime import datetime # Para calcular la edad con precisión

# --- MÓDULO DE CALIDAD (CODEBOOK) ---
def codebook(df, pk_col='Número de Documento'):
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
        # Asegurarse de que el DNI esté limpio (sin nulos) para la verificación de PK
        total_dups_dni = df[pk_col].dropna().duplicated().sum()
        if total_dups_dni > 0:
            print(f"   -> ALERTA PK ({pk_col}): {total_dups_dni} duplicados encontrados.")
            resumen.loc[pk_col, "Duplicados (Valores)"] = f"¡SÍ! ({total_dups_dni} duplicados)"
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

def categorizar_discapacidad(valor_str):
    """
    Normaliza y categoriza la columna de texto libre '¿Qué discapacidad tiene?'.
    """
    if pd.isna(valor_str):
        return np.nan

    # Limpiar el texto
    s = str(valor_str).upper().strip()

    if s in ['NADA', 'NINGUNA', 'NINGUNO']: return 'NINGUNA'
    if 'LENGUAJE' in s: return 'LENGUAJE'
    if any(term in s for term in ['RETARDO', 'APRENDISAJE', 'TDH', 'FRONTERIZO']): return 'APRENDIZAJE'
    if 'AUTISMO' in s: return 'TEA'
    if any(term in s for term in ['VISION', 'ASTIGMATISMO', 'VISTA']): return 'VISUAL'
    if any(term in s for term in ['DISPLACIA', 'MICROTIA', 'CADERA']): return 'FISICA'
    if s: return 'OTRO'

    return np.nan

def limpiar_y_deduplicar_datos(file_path, output_filename_csv, output_filename_excel):
    """
    CONSULTORA: Script de Limpieza y Deduplicación de Datos. (Versión 5.1 - Keep Last + Calidad)

    ACTUALIZACIÓN:
    1. REGLA DE DEDUPLICACIÓN: Conservar ÚNICAMENTE el registro más reciente
       según la 'Marca temporal' para DNI de niño duplicados.
    2. Se añade la columna 'EDAD' calculada dinámicamente.
    3. Se categoriza la columna '¿Qué discapacidad tiene?'.
    4. Se anulan las fechas de nacimiento inválidas.
    5. Se genera un reporte de calidad (Codebook) del output final.
    """

    print(f"--- Iniciando Proceso de Limpieza y Deduplicación para: {file_path} ---")

    try:
        # --- PASO 1: Carga de Datos ---
        df = pd.read_excel(file_path)
        filas_originales = len(df)
        print(f"Archivo original cargado. Filas: {filas_originales}, Columnas: {len(df.columns)}")

    except FileNotFoundError:
        print(f"--- ERROR CRÍTICO ---\nArchivo no encontrado en la ruta: {file_path}")
        return
    except Exception as e:
        print(f"Error inesperado al leer el archivo Excel: {e}")
        return

    # --- PASO 2: Limpieza Previa Obligatoria ---
    print("Realizando limpieza previa de campos clave...")

    # [A] Limpiar DNI Niño (clave principal de duplicados)
    if 'Número de Documento' in df.columns:
        df['Número de Documento'] = (
            df['Número de Documento']
            .astype(str)
            .str.replace(r'\\.0$', '', regex=True) # Elimina ".0" de floats
            .str.replace(r'\\D', '', regex=True)
        )
        df['Número de Documento'] = df['Número de Documento'].replace(['nan', '0', ''], np.nan)

    # [B] Convertir Marca Temporal (para ordenar)
    if 'Marca temporal' in df.columns:
        df['Marca temporal'] = pd.to_datetime(df['Marca temporal'], errors='coerce')

    # [C] Convertir 'Fecha de nacimiento' y calcular EDAD sin anular valores
    print("Convirtiendo 'Fecha de nacimiento' y calculando EDAD...")

    if 'Fecha de nacimiento' in df.columns:

        # Convertir a datetime (solo coerción, sin reglas adicionales)
        df['Fecha de nacimiento'] = pd.to_datetime(
            df['Fecha de nacimiento'],
            errors='coerce',
            dayfirst=True
        )

        # Calcular EDAD solo cuando la fecha es válida
        fecha_hoy = pd.Timestamp(datetime.now().date())

        df['EDAD'] = (
            (fecha_hoy - df['Fecha de nacimiento'])
            .dt.days
            .div(365.25)
            .apply(lambda x: int(x) if pd.notnull(x) else pd.NA)
            .astype("Int64")
        )

        print("Fecha de nacimiento convertida y EDAD calculada (sin anular ninguna fecha).")


    # [D] Limpiar TODOS los campos de texto (object)
    print("Limpiando campos de texto...")
    for col in df.select_dtypes(include=['object']).columns:
        if col not in ['Número de Documento', 'Número de Documento.1', 'Número de celular']:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r'\\.0$', '', regex=True)
                .str.upper()
                .str.strip()
                .replace('NAN', np.nan)
                .replace('', np.nan) # Reemplazar strings vacíos
            )

    # [F] Limpiar IDs de Apoderado y Celular
    cols_numeros_id_otros = ['Número de Documento.1', 'Número de celular']
    for col in cols_numeros_id_otros:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r'\\.0$', '', regex=True)
                .str.replace(r'\\D', '', regex=True)
            )
            df[col] = df[col].replace(['nan', '0', ''], np.nan)

    # --- NUEVO PASO 3: Categorización de Discapacidad ---
    print("Categorizando columna '¿Qué discapacidad tiene?'...")
    if '¿Qué discapacidad tiene?' in df.columns:
        # La limpieza de texto (mayúsculas/strip) ya se hizo en el PASO 2 [D]
        df['¿Qué discapacidad tiene?'] = df['¿Qué discapacidad tiene?'].apply(categorizar_discapacidad)

    # --- PASO 4: Identificar Grupos de Datos (Únicos vs Duplicados vs Nulos) ---

    # 1. Identificar DNIs duplicados (excluyendo Nulos)
    mask_duplicados = df['Número de Documento'].duplicated(keep=False) & df['Número de Documento'].notnull()

    # 2. Separar el DataFrame
    df_duplicados_raw = df[mask_duplicados].copy()

    # 3. Los registros únicos y los nulos se conservan tal cual
    df_unicos_y_nulos = df[~mask_duplicados].copy()

    print(f"Registros únicos y nulos (se conservan): {len(df_unicos_y_nulos)}")
    print(f"Registros con DNI duplicado (a procesar): {len(df_duplicados_raw)}")

    # --- PASO 5: Resolver Duplicados (Conservar el último registro) ---

    df_resueltos = pd.DataFrame(columns=df.columns)

    if not df_duplicados_raw.empty:
        print("Procesando duplicados (ordenando por 'Marca temporal')...")

        # 1. Ordenar TODOS los duplicados por fecha, de más antiguo a más reciente
        df_duplicados_ordenados = df_duplicados_raw.sort_values(by='Marca temporal', ascending=True, na_position='first')

        # 2. Quedarse solo con el ÚLTIMO registro de cada DNI
        df_resueltos = df_duplicados_ordenados.drop_duplicates(subset=['Número de Documento'], keep='last')

        print(f"Registros conservados tras el proceso de deduplicación: {len(df_resueltos)}")

    # --- PASO 6: Reconstruir y Guardar el DataFrame ---

    # 1. Unir los únicos/nulos con los duplicados ya resueltos
    df_final = pd.concat([df_unicos_y_nulos, df_resueltos], ignore_index=True)

    # 2. Ordenar por el índice original para mantener el orden del archivo
    df_final = df_final.sort_index()
    filas_finales = len(df_final)

    # --- NUEVO (Solicitud Usuario): Convertir Marca temporal a solo fecha ---
    if 'Marca temporal' in df_final.columns:
        df_final['Marca temporal'] = df_final['Marca temporal'].dt.date

    print("Proceso de deduplicación completado.")

    # --- PASO 7: Guardado del Archivo Limpio (CSV) ---
    try:
        output_dir = os.path.dirname(output_filename_csv)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Se creó el directorio de salida: {output_dir}")

        df_final.to_csv(output_filename_csv, index=False, encoding='utf-8-sig')

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

    # --- NUEVO PASO 8: Guardado del Reporte de Calidad (Excel) ---
    print("\n📊 Generando reporte de calidad de datos (Codebook)...")

    # Llamar a la función codebook con el DataFrame FINAL y LIMPIO
    # Usamos 'Número de Documento' como la PK para el reporte
    df_calidad = codebook(df_final, pk_col='Número de Documento')

    # Guardar el reporte de calidad en un Excel
    try:
        df_calidad.to_excel(output_filename_excel, index=False, engine="openpyxl")
        print(f"✅ Reporte de calidad guardado en:\n{output_filename_excel}")
    except PermissionError:
        print(f"\n--- ERROR AL GUARDAR (Excel Calidad) ---")
        print(f"Permiso denegado al intentar guardar en: {output_filename_excel}")
        print("Acción Requerida: Cierre el archivo si lo tiene abierto en Excel e intente de nuevo.")
    except Exception as e:
        print(f"[ERROR] No se pudo guardar el reporte de calidad: {e}")



#### Aplicacion: 

# --- BLOQUE FINAL PARA EJECUCIÓN LOCAL -------------------------------------

if __name__ == "__main__":

    # 1. Archivo descargado automáticamente desde Google Drive
    input_path = "entrada/Solicitud_matricula.xlsx"

    # 2. Carpeta de salida dentro del proyecto
    output_folder = "salida"
    os.makedirs(output_folder, exist_ok=True)

    # 3. Archivos de salida
    output_path_csv   = os.path.join(output_folder, "Sol_Mtr_Deduplicado.csv")
    output_path_excel = os.path.join(output_folder, "Sol_Mtr_Deduplicado_CALIDAD.xlsx")

    print(f"🚀 Ejecutando limpieza sobre: {input_path}")
    
    try:
        limpiar_y_deduplicar_datos(
            file_path=input_path,
            output_filename_csv=output_path_csv,
            output_filename_excel=output_path_excel
        )

        print("\n✅ ¡Proceso finalizado con éxito!")
        print(f"📂 CSV limpio guardado en: {output_path_csv}")
        print(f"📊 Reporte de calidad guardado en: {output_path_excel}")

    except Exception as e:
        print(f"\n❌ Error durante la ejecución lógica: {e}")






