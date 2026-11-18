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
después de comprobar un respaldo completo y su subida a GitHub.

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
El workflow habitual se inicia manualmente. Por defecto solo prepara y respalda;
la opción `cargar_salesforce` permite el reemplazo exclusivamente desde `main`.
En `Fixing-etl-rc`, los cambios de la protección ejecutan preparación y respaldo,
pero nunca el job de carga.

Los artefactos generados se eliminan automaticamente despues de 1 dia para evitar saturacion de almacenamiento.
Los respaldos de Salesforce y resultados de carga se conservan 7 días.

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

Solicitud Matrícula conserva `Edad actual` y `Edad de ingreso` con sus nombres
originales en el JSON destinado a Salesforce. Por excepción explícita, si esos
dos campos no están homologados o no son insertables, la validación previa
emite un aviso y continúa. Los demás campos mantienen la validación estricta.
Esto no garantiza que Salesforce los acepte. Para ese envío, el coordinador
inserta y comprueba todos los resultados antes de borrar los registros anteriores
de Solicitud Matrícula. Un rechazo remoto sigue siendo un error; los registros
anteriores permanecen y cualquier inserción parcial queda en el informe de lotes.

En la rama `Fixing-etl-rc`, el workflow **Validar ETL 2025 y 2026 sin Salesforce**
se activa con cambios en el ETL, sus pruebas o el workflow de validación.
Ejecuta las descargas, transformaciones y consolidaciones de los nueve procesos
activos de `etl.yml`; no ejecuta los scripts de `carga/` ni recibe secrets `SF_*`.
Ficha Social y Solicitud Matrícula se descargan y transforman una sola vez:
sus scripts de ambos años apuntan al mismo Google Sheets. Se reutilizan los
scripts y rutas de trabajo de `2026`, sin filtrar por año. Sus preparadores
finales leen únicamente esa copia, conservan las rutas finales de carga y no
añaden `ANIO_FUENTE` a partir de la carpeta. Las copias de `2025` no se incorporan.
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

## Carga protegida, respaldo y recuperación

`ETL-Automatico-Salesforce` ejecuta en este orden:

1. Descarga, transforma y consolida todos los procesos activos de ambos años.
2. Prepara todos los registros; rechaza archivos vacíos, columnas duplicadas,
   campos desconocidos/no insertables, valores obligatorios ausentes, longitudes
   excedidas y tipos básicos inválidos según la metadata de Salesforce.
3. Exporta los registros actuales completos, sus IDs y metadata de los objetos
   que se reemplazarían. Guarda `backups/salesforce/*.json.gz` y `manifest.json`.
4. Sube el artefacto `salesforce-backup-<run_id>-<intento>`. Si falla su subida,
   el job de carga no puede comenzar. Los JSON preparados se guardan aparte.
5. Solo si se pidió carga manual desde `main`, recupera esos mismos archivos y
   verifica organización, ejecución/intento, antigüedad (máximo 2 horas), hashes,
   schema actual e IDs/SystemModstamp de Salesforce antes de borrar.
6. Espera los resultados de cada lote de delete e insert y comprueba los IDs
   finales. Un lote parcialmente rechazado marca error. Los resultados e IDs
   insertados quedan en `data/load-results.json`, subido incluso ante fallo.

La concurrencia del workflow permite una sola ejecución a la vez y no cancela
una carga en curso. No protege contra otros sistemas o usuarios que escriban
directamente en Salesforce durante el reemplazo. Las reglas de validación,
triggers y permisos del servidor pueden rechazar registros aunque pasen las
comprobaciones previas; el reemplazo de varios objetos no es una transacción
única y no hay restauración automática.

Si falla una carga, no la repitas a ciegas. Conserva los artefactos de respaldo
y resultados, y revisa los jobs/lotes identificados en el informe. El backup
incluye campos de solo lectura para diagnóstico: no se debe insertar su JSON
sin seleccionar campos escribibles. El borrado es el delete normal de Bulk API,
no hard delete. Un administrador debe evaluar la recuperación desde la papelera
para conservar IDs, o reconstruir registros y relaciones a partir del respaldo.
Reinsertar registros puede generar IDs nuevos. Revertir Git no restaura datos.
Descarga el backup antes de que venza su retención y protégelo como los datos
personales de Salesforce que contiene.

Para repetir una operación fallida, usa **Re-run all jobs**: así se obtienen
preparados y un backup nuevos. No se permite cargar con un respaldo de otro
intento ni con archivos preparados modificados.
