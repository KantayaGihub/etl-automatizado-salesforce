# ETL Automatizado - Salesforce Kantaya

Pipeline de datos para la extracción, transformación, consolidación y carga de registros hacia Salesforce. Procesa múltiples fuentes Excel con estructuras heterogéneas y centraliza la información en 11 objetos personalizados de Salesforce.

---

## Arquitectura

```
Google Drive / SharePoint
        |
        v
   Extracción (Service Account)
        |
        v
   data/raw/
        |
        v
   Transformación (Python + Pandas)
        |
        v
   data/consolidated/
        |
        v
   Carga Bulk API (Salesforce)
        |
        v
   Objetos Salesforce (11 entidades)
```

---

## Objetos Salesforce gestionados

| Script                              | Objeto Salesforce                             |
| ----------------------------------- | --------------------------------------------- |
| 01_asistencia_regular               | `Asistencias_e7__c`                         |
| 02_asistencia_extracurricular       | `Asistencias_Extracurriculares_e__c`        |
| 03_matricula                        | `Matricula_e__c`                            |
| 04_calificaciones_area_competencias | `Calificaciones_areas_e__c`                 |
| 05_encuesta_satisfaccion_padres     | `Formulario_SatisfaccionInicial6grado_e__c` |
| 06_ficha_social                     | `Ficha_social_e__c`                         |
| 07_progreso_curricular              | `Progreso_curricular_e__c`                  |
| 08_solicitud_matricula              | `Formulario_SolicitudMatricula_e__c`        |
| 09_horario_regular                  | `horario_regular__c`                        |
| 10_asistencia_act_vivenciales       | `asistencia_actividades_vivenciales__c`     |
| 11_habilidades                      | `BD_Habilidades_e__c`                       |

Los scripts individuales de `carga/` ahora solo preparan y validan los registros
contra la metadata de Salesforce, sin borrar ni insertar. Guardan JSON en
`data/prepared/`. El coordinador `_carga_segura.py` realiza el reemplazo únicamente
después de validar todos los registros preparados. No genera backup de Salesforce.

---

## Estructura del repositorio

```
.
├── etl/
│   └── procesos/
│       ├── carga/              # Scripts de carga a Salesforce (01-11)
│       ├── consolidacion/      # Scripts de consolidacion por proceso
│       └── 2025/ 2026/         # Scripts de extraccion y transformacion por anio
│
├── data/
│   ├── raw/                    # Archivos descargados desde Drive/SharePoint
│   ├── processed/              # Datos transformados por proceso
│   └── consolidated/           # CSVs listos para carga a Salesforce
│
├── .github/workflows/
│   └── etl.yml                 # Pipeline de GitHub Actions
│
├── requirements.txt
└── README.md
```

---

## Automatizacion

La programación automática está pausada durante la primera carga protegida.
El workflow habitual se inicia manualmente. Por defecto solo prepara y valida;
la opción `cargar_salesforce` permite el reemplazo exclusivamente desde `main`.
En `Fixing-etl-rc`, los cambios de la protección ejecutan preparación y validación,
pero nunca el job de carga.

Los artefactos generados se eliminan automaticamente despues de 1 dia para evitar saturacion de almacenamiento.
Los informes de resultados de carga se conservan 7 días; no contienen un backup de Salesforce.

---

## Configuracion de credenciales

Las credenciales se gestionan como secrets de GitHub Actions y variables de entorno locales:

| Variable              | Descripcion                     |
| --------------------- | ------------------------------- |
| `SF_USERNAME`       | Usuario de Salesforce           |
| `SF_PASSWORD`       | Contrasena de Salesforce        |
| `SF_SECURITY_TOKEN` | Token de seguridad de la cuenta |

Para ejecucion local, definirlas antes de ejecutar cualquier script de carga:

```bash
export SF_USERNAME="tu_usuario@org.com"
export SF_PASSWORD="tu_contrasena"
export SF_SECURITY_TOKEN="tu_token"
```

---

## Dependencias

```
simple-salesforce
salesforce-bulk
pandas
numpy
openpyxl
gspread
google-auth
```

Instalar con:

```bash
pip install -r requirements.txt
```

---

## Validar ambos años sin cargar a Salesforce

Solicitud Matrícula conserva `Edad actual` y `Edad de ingreso` en los archivos
fuente, el consolidado y `DEBUG_para_salesforce.csv`, pero las excluye del JSON
preparado para Salesforce porque no tienen campos homologados. `EDAD__c`, calculada
desde la fecha de nacimiento, se sigue enviando. La validación rechaza cualquier
campo desconocido o no insertable, sin excepciones para las dos edades.
El orden de carga sigue siendo borrar primero e insertar después. Los errores
reales de Salesforce se conservan en el informe de lotes.

En la rama `Fixing-etl-rc`, el workflow **Validar ETL 2025 y 2026 sin Salesforce**
se activa con cambios en el ETL, sus pruebas o el workflow de validación.
Ejecuta las descargas, transformaciones y consolidaciones de los nueve procesos
activos de `etl.yml`; no ejecuta los scripts de `carga/` ni recibe secrets `SF_*`.
Ficha Social y Solicitud Matrícula se descargan y transforman una sola vez:
sus scripts de ambos años apuntan al mismo Google Sheets. Se reutilizan los
scripts y rutas de `2026` para Ficha Social y de `2025` para Solicitud Matrícula,
sin filtrar por año. Cada preparador final lee solo su copia y conserva la ruta
final de carga. Solicitud Matrícula vuelve a añadir `ANIO_FUENTE = 2025` para
reproducir el payload de `31b0ba9`; es una etiqueta de compatibilidad, no una
clasificación del año real de cada registro. Ficha Social no añade esa etiqueta.
Continúa con las demás etapas si alguna falla, para recoger evidencia de todos
los procesos. Los estados de los pasos y las cantidades de filas quedan en
`resultados-validacion/resumen.md` y `resumen.json`.

En la ejecución de Actions, descarga el artefacto
`validacion-etl-2025-2026-<ejecución>-<intento>`, disponible durante un día.
Contiene `data/` con fuentes, archivos procesados y consolidados, además de los
informes. La subida se intenta incluso si hubo errores; el resultado final se
marca como fallido si un paso falla o se detectan salidas vacías o ilegibles.
Un resultado correcto no certifica la calidad de las filas ni autoriza su carga
a Salesforce. El workflow habitual `etl.yml` sigue teniendo las cargas reales.

## Consultar cantidades de Salesforce sin ejecutar el ETL

El workflow **Consultar cantidades Salesforce (solo lectura)** ejecuta únicamente
`scripts/contar_salesforce.py`. Usa los secrets `SF_USERNAME`, `SF_PASSWORD` y
`SF_SECURITY_TOKEN` para realizar `SELECT COUNT()` en los once objetos conocidos
del ETL, indicando los nueve activos y los dos inactivos. No descarga las fuentes,
no transforma archivos del ETL, no ejecuta cargadores y no modifica Salesforce.

En `Testeo-data-sf` se activa al hacer push de cambios en el diagnóstico o sus
pruebas. Para repetirlo, abre su ejecución en Actions y pulsa **Re-run all jobs**;
solo se repite este diagnóstico. También declara ejecución manual para cuando
el workflow esté disponible en la rama predeterminada de GitHub.

Las cantidades aparecen en el log del paso **Consultar cantidades actuales** y
en el **Summary** de la ejecución. El artefacto `conteo-salesforce-<run>-<intento>`
se descarga como ZIP con `resumen.csv` (abrible con Excel), `resumen.json` y
`resumen.md`, disponible siete días. No contiene registros individuales. Los
conteos incluyen los registros visibles para el usuario de integración, sin la
papelera; no representan personas únicas. Un error de un objeto se informa y
se continúa con los siguientes; al final la ejecución queda fallida si hubo errores
en objetos activos o al conectar. Un objeto inactivo no disponible se muestra
como advertencia, sin inventar una cantidad de cero.

## Carga validada sin backup

Al finalizar la consolidación de Matrícula se eliminan filas idénticas comparando
todas sus columnas salvo `ANIO_FUENTE`. Si la misma fila aparece en ambos años,
se conserva la de 2025. Las filas con el mismo DNI y otros datos distintos se
mantienen; el log muestra cuántas filas duplicadas se eliminaron.

`ETL-Automatico-Salesforce` ejecuta en este orden:

1. Descarga, transforma y prepara los archivos finales de los procesos activos.
2. Valida los registros contra la metadata de Salesforce y guarda JSON preparados.
   Rechaza vacíos, columnas duplicadas, campos desconocidos, tipos inválidos y requisitos incumplidos.
3. Guarda los archivos preparados como artefactos de esta ejecución.
4. Solo con carga manual habilitada desde `main`, recupera esos JSON y vuelve a
   validar todos los objetos antes de realizar cualquier escritura.
5. Consulta los IDs actuales para reemplazar los registros. No exporta sus datos
   ni genera, sube o requiere un backup de Salesforce.
6. Espera los resultados de cada lote y verifica los IDs finales. Los errores e
   IDs insertados se guardan en `data/load-results.json`, subido incluso ante fallo.

Todos los objetos, incluida Solicitud Matrícula, usan el orden histórico de
borrar primero e insertar después. Los resultados de ambas operaciones se comprueban.
La concurrencia permite una ejecución a la vez y no cancela una carga en curso.
El reemplazo de varios objetos no es una transacción única ni tiene restauración
automática. Si falla, se conservan los informes de lotes, no una copia de los datos
anteriores. Volver a ejecutar `main` carga las fuentes disponibles en ese momento;
revertir un commit de Git no restaura el estado anterior de Salesforce.
