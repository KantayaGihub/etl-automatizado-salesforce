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

Cada script ejecuta un ciclo completo: elimina registros existentes, espera confirmación de la API, e inserta el consolidado actualizado. El delete y el insert utilizan la misma instancia de `SalesforceBulk` para garantizar que la eliminación finalice antes de comenzar la inserción.

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

El pipeline se ejecuta automaticamente mediante GitHub Actions de lunes a viernes en los horarios: 08:00, 12:00, 16:00 y 18:00 (hora local configurada en el workflow).

Los artefactos generados se eliminan automaticamente despues de 1 dia para evitar saturacion de almacenamiento.

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
