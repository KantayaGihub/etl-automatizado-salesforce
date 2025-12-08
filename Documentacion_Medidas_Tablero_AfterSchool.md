# Documentación de Medidas - Tablero AfterSchool v5
## Informe Gerencial

**Fecha de generación:** 8 de diciembre de 2025  
**Total de medidas:** 101  
**Modelo:** Tablero_AfterSchool_v5

---

## Índice de Contenidos
1. [Métricas de Asistencia](#1-métricas-de-asistencia)
2. [Métricas de Niños y Participantes](#2-métricas-de-niños-y-participantes)
3. [Métricas de Evaluación y Calificaciones](#3-métricas-de-evaluación-y-calificaciones)
4. [Métricas de Satisfacción](#4-métricas-de-satisfacción)
5. [Métricas de Habilidades](#5-métricas-de-habilidades)
6. [Métricas de Talleres y Actividades](#6-métricas-de-talleres-y-actividades)
7. [Métricas de Impacto y Variación](#7-métricas-de-impacto-y-variación)
8. [Métricas de Visualización y Formato](#8-métricas-de-visualización-y-formato)

---

## 1. Métricas de Asistencia

### 1.1 Asistencias_acumuladas
**Descripción:** Cuenta el total de asistencias registradas con estado "A" (Asistió).  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(COUNTROWS(Asistencias_e7), 
          FILTER(Asistencias_e7, Asistencias_e7[ASISTENCIA__c]="A"))
```
**Uso:** Métrica fundamental para medir la asistencia total en el programa.

---

### 1.2 Asistencias_acumuladas extracurriculares
**Descripción:** Cuenta el total de asistencias a actividades extracurriculares con estado "A".  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(COUNTROWS(Asistencias_Extracurriculares_e), 
          FILTER(Asistencias_Extracurriculares_e, 
                 Asistencias_Extracurriculares_e[ASISTENCIA__c]="A"))
```
**Uso:** Medición de participación en actividades complementarias.

---

### 1.3 Asistencia_Acumulada Corte 1
**Descripción:** Asistencias acumuladas hasta la fecha de corte 1 seleccionada.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR Corte = SELECTEDVALUE(CalendarioFiltro1[Date])
RETURN
CALCULATE(
    COUNTROWS(Asistencias_e7),
    Asistencias_e7[ASISTENCIA__c] = "A",
    Asistencias_e7[FECHA__c] <= Corte,
    ALL(Asistencias_e7[FECHA__c])
)
```
**Uso:** Seguimiento de asistencias en periodos específicos.

---

### 1.4 Asistencia_Acumulada Corte 2
**Descripción:** Asistencias acumuladas hasta la fecha de corte 2 seleccionada.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR Corte = SELECTEDVALUE(CalendarioFiltro2[Date])
RETURN
CALCULATE(
    COUNTROWS(Asistencias_e7),
    Asistencias_e7[ASISTENCIA__c] = "A",
    Asistencias_e7[FECHA__c] <= Corte,
    ALL(Asistencias_e7[FECHA__c])
)
```

---

### 1.5 Asistencia_Acumulada Corte 3
**Descripción:** Asistencias extracurriculares acumuladas hasta la fecha de corte 3.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR Corte = SELECTEDVALUE(CalendarioFiltro3[Date])
RETURN
CALCULATE(
    COUNTROWS(Asistencias_Extracurriculares_e),
    Asistencias_Extracurriculares_e[ASISTENCIA__c] = "A",
    Asistencias_Extracurriculares_e[FECHA__c] <= Corte,
    ALL(Asistencias_Extracurriculares_e[FECHA__c])
)
```

---

### 1.6 Asistencia_Acumulada Corte 4
**Descripción:** Asistencias extracurriculares acumuladas hasta la fecha de corte 4.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR Corte = SELECTEDVALUE(CalendarioFiltro4[Date])
RETURN
CALCULATE(
    COUNTROWS(Asistencias_Extracurriculares_e),
    Asistencias_Extracurriculares_e[ASISTENCIA__c] = "A",
    Asistencias_Extracurriculares_e[FECHA__c] <= Corte,
    ALL(Asistencias_Extracurriculares_e[FECHA__c])
)
```

---

### 1.7 Porcentaje_Asistencia
**Descripción:** Calcula el porcentaje de asistencias reales sobre asistencias esperadas por alumno.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
VAR Reales =
    CALCULATE(
        DISTINCT(Asistencias_e7[A_REALES__c]),
        ALLEXCEPT(Asistencias_e7, Asistencias_e7[DNI__c])
    )
VAR Esperadas =
    CALCULATE(
        DISTINCT(Asistencias_e7[A_ESPERADAS__c]),
        ALLEXCEPT(Asistencias_e7, Asistencias_e7[DNI__c])
    )
RETURN
DIVIDE(Reales, Esperadas)
```
**Uso:** KPI principal de asistencia individual.

---

### 1.8 Promedio_Asistencia_Global
**Descripción:** Promedio de porcentaje de asistencia de todos los alumnos.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
AVERAGEX(
    VALUES(Asistencias_e7[DNI__c]),
    VAR Reales =
        CALCULATE(
            MAX(Asistencias_e7[A_REALES__c]),
            ALLEXCEPT(Asistencias_e7, Asistencias_e7[DNI__c])
        )
    VAR Esperadas =
        CALCULATE(
            MAX(Asistencias_e7[A_ESPERADAS__c]),
            ALLEXCEPT(Asistencias_e7, Asistencias_e7[DNI__c])
        )
    RETURN
        DIVIDE(Reales, Esperadas)
)
```
**Uso:** KPI agregado de asistencia general del programa.

---

### 1.9 Porcentaje_de faltas
**Descripción:** Promedio del porcentaje de faltas sobre asistencias esperadas.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
AVERAGEX(
    VALUES(Asistencias_e7[DNI__c]),
    VAR Faltas =
        CALCULATE(
            COUNTROWS(
                FILTER(
                    Asistencias_e7,
                    Asistencias_e7[ASISTENCIA__c] = "F"
                )
            )
        )
    VAR Esperadas =
        CALCULATE(
            MAX(Asistencias_e7[A_ESPERADAS__c])
        )
    RETURN
        DIVIDE(Faltas, Esperadas)
)
```

---

### 1.10 Porcentaje_de faltas justificadas
**Descripción:** Promedio del porcentaje de faltas justificadas sobre asistencias esperadas.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
AVERAGEX(
    VALUES(Asistencias_e7[DNI__c]),
    VAR Faltas =
        CALCULATE(
            COUNTROWS(
                FILTER(
                    Asistencias_e7,
                    Asistencias_e7[ASISTENCIA__c] = "FJ"
                )
            )
        )
    VAR Esperadas =
        CALCULATE(
            MAX(Asistencias_e7[A_ESPERADAS__c])
        )
    RETURN
        DIVIDE(Faltas, Esperadas)
)
```

---

### 1.11 Numero_de clases
**Descripción:** Cuenta el número de fechas únicas de clases registradas.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(
    DISTINCTCOUNT(Asistencias_e7[FECHA__c])
)
```
**Uso:** Métrica para conocer la cantidad de sesiones realizadas.

---

## 2. Métricas de Niños y Participantes

### 2.1 Niños_Kantaya
**Descripción:** Cuenta de niños únicos participantes del programa (activos e inactivos).  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR _valor =
    CALCULATE(
    DISTINCTCOUNT(Asistencias_e7[DNI__c]),
    Asistencias_e7[TIPO_ALUMNO__c] IN {"ACTIVO", "INACTIVO"})
RETURN
IF ( ISBLANK(_valor), 0, _valor )
```
**Uso:** KPI fundamental - Total de beneficiarios del programa.

---

### 2.2 Niños_activos KANTAYA
**Descripción:** Cuenta de niños con estado "ACTIVO" en el programa.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(
    DISTINCTCOUNT(Asistencias_e7[DNI__c]),
    Asistencias_e7[TIPO_ALUMNO__c] = "ACTIVO"
)
```
**Uso:** KPI de retención - Niños actualmente participando.

---

### 2.3 Niños_inactivos KANTAYA
**Descripción:** Cuenta de niños con estado "INACTIVO" en el programa.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(
    DISTINCTCOUNT(Asistencias_e7[DNI__c]),
    Asistencias_e7[TIPO_ALUMNO__c] = "INACTIVO"
)
```
**Uso:** Métrica de deserción.

---

### 2.4 Niños_Kantaya Extracurriculares
**Descripción:** Niños participantes en actividades extracurriculares (activos e inactivos).  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR _valor =
    CALCULATE(
        DISTINCTCOUNT(Asistencias_Extracurriculares_e[DNI__c]),
        FILTER(
            Asistencias_Extracurriculares_e,
            Asistencias_Extracurriculares_e[TIPO_ALUMNO__c] IN {"ACTIVO","INACTIVO"}
        )
    )
RETURN
IF ( ISBLANK(_valor), 0, _valor )
```

---

### 2.5 Niños_con discapacidad
**Descripción:** Cuenta de niños que reportan tener alguna discapacidad.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
COUNTROWS(
    FILTER(
        VALUES(Formulario_SolicitudMatricula_e[Nmero_de_Documento__c]),
        CALCULATE(
            SELECTEDVALUE(Formulario_SolicitudMatricula_e[Cuenta_con_algn_tipo_de_discapacidad__c])
        ) = "SÍ"
    )
)
```
**Uso:** Métrica de inclusión.

---

### 2.6 Niños_en la ficha social
**Descripción:** Cantidad de niños registrados en la ficha social.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR _valor =
    CALCULATE( COUNTROWS('Ficha_social_niños_e') )
RETURN
IF ( ISBLANK(_valor), 0, _valor )
```

---

### 2.7 Ultima_incorporación Kantaya
**Descripción:** Número de niños incorporados en el último mes registrado.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR UltimaFecha =
    CALCULATE(
        MAX(Asistencias_e7[F_INCORPORACION__c]),
        ALL(Asistencias_e7)
    )
VAR UltimoMesInicio =
    DATE(YEAR(UltimaFecha), MONTH(UltimaFecha), 1)
VAR UltimoMesFin =
    EOMONTH(UltimaFecha, 0)
RETURN
CALCULATE(
    DISTINCTCOUNT(Asistencias_e7[DNI__c]),
    FILTER(
        Asistencias_e7,
        Asistencias_e7[F_INCORPORACION__c] >= UltimoMesInicio
            && Asistencias_e7[F_INCORPORACION__c] <= UltimoMesFin
    )
)
```

---

### 2.8 Nuevos_activos_mes
**Descripción:** Niños que se activaron en el mes actual.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR _MesActual = MAX(Asistencias_e7[FECHA__c])
RETURN
CALCULATE(
    DISTINCTCOUNT(Asistencias_e7[DNI__c]),
    FILTER(
        VALUES(Asistencias_e7[DNI__c]),
        NOT ISBLANK([MesActivacion]) &&
        YEAR([MesActivacion]) = YEAR(_MesActual) &&
        MONTH([MesActivacion]) = MONTH(_MesActual)
    )
)
```

---

### 2.9 Nuevos_inactivos_mes
**Descripción:** Niños que se desactivaron en el mes actual.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR _MesActual = MAX(Asistencias_e7[FECHA__c])
RETURN
CALCULATE(
    DISTINCTCOUNT(Asistencias_e7[DNI__c]),
    FILTER(
        VALUES(Asistencias_e7[DNI__c]),
        NOT ISBLANK([MesDesercion]) &&
        YEAR([MesDesercion]) = YEAR(_MesActual) &&
        MONTH([MesDesercion]) = MONTH(_MesActual)
    )
)
```

---

### 2.10 Niños_impactados_mes
**Descripción:** Total de niños impactados en el mes (nuevos activos + nuevos inactivos).  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
[Nuevos_activos_mes] + [Nuevos_inactivos_mes]
```

---

### 2.11 MesActivacion
**Descripción:** Fecha de primera activación de un niño en el programa.  
**Tipo de dato:** Fecha/Hora  
**Fórmula DAX:**
```dax
CALCULATE(
    MIN(Asistencias_e7[FECHA__c]),
    FILTER(
        ALLEXCEPT(Asistencias_e7, Asistencias_e7[DNI__c]),
        Asistencias_e7[TIPO_ALUMNO__c] = "ACTIVO"
    )
)
```

---

### 2.12 MesDesercion
**Descripción:** Fecha de inactivación de un niño en el programa.  
**Tipo de dato:** Fecha/Hora  
**Fórmula DAX:**
```dax
CALCULATE(
    MIN(Asistencias_e7[FECHA__c]),
    FILTER(
        ALLEXCEPT(Asistencias_e7, Asistencias_e7[DNI__c]),
        Asistencias_e7[TIPO_ALUMNO__c] = "INACTIVO"
    )
)
```

---

### 2.13 Porcentaje_de retencion
**Descripción:** Porcentaje de niños activos sobre el total de niños.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
DIVIDE(
    [Niños_activos KANTAYA],
    [Niños_Kantaya]
)
```
**Uso:** KPI de retención del programa.

---

### 2.14 Porcentaje_de desercion
**Descripción:** Porcentaje de niños inactivos sobre el total de niños.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
DIVIDE(
    [Niños_inactivos KANTAYA],
    [Niños_Kantaya]
)
```
**Uso:** KPI de deserción del programa.

---

### 2.15 Apoderados_Kantaya
**Descripción:** Cuenta de apoderados únicos registrados.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(
    DISTINCTCOUNT(Ficha_social_Apoderados_e[Apoderado_DNI__c])
)
```

---

### 2.16 Numero_familias
**Descripción:** Número de familias beneficiarias del programa.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(
    DISTINCTCOUNT(Ficha_social[Número de Documento])
)
```

---

## 3. Métricas de Evaluación y Calificaciones

### 3.1 Niños evaluados_por areas
**Descripción:** Cuenta de niños únicos evaluados en áreas curriculares.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(
    DISTINCTCOUNT(Calificaciones_areas_e[DNI__c])
)
```

---

### 3.2 Niños evaluados total_(ignora filtros)
**Descripción:** Total de niños evaluados ignorando filtros de contexto.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(
    DISTINCTCOUNT('Calificaciones_areas_e'[DNI__c]),
    ALL('Calificaciones_areas_e')
)
```

---

### 3.3 %_Alumnos con nivel alcanzado
**Descripción:** Porcentaje de alumnos que alcanzaron un nivel de logro específico.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
DIVIDE(
    DISTINCTCOUNT('Calificaciones_areas_e'[DNI__c]),
    CALCULATE(
        DISTINCTCOUNT('Calificaciones_areas_e'[DNI__c]),
        REMOVEFILTERS('Calificaciones_areas_e'[NIVEL_DE_LOGRO__c])
    ),
    0
)
```
**Uso:** KPI de logro académico.

---

### 3.4 Alumnos_únicos por nivel
**Descripción:** Cuenta de alumnos únicos por cada nivel de logro.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(
    DISTINCTCOUNT('Calificaciones_areas_e'[DNI__c]),
    VALUES('Calificaciones_areas_e'[NIVEL_DE_LOGRO__c])
)
```

---

### 3.5 Promedio_Ciencia
**Descripción:** Promedio de calificaciones en Ciencia y Tecnología.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
CALCULATE(
    AVERAGE('Calificaciones_areas_e'[Valor_Promedio__c]),
    'Calificaciones_areas_e'[REA__c] = "Ciencia y Tecnología"
)
```

---

### 3.6 Promedio_Comunicacion
**Descripción:** Promedio de calificaciones en Comunicación.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
CALCULATE(
    AVERAGE('Calificaciones_areas_e'[Valor_Promedio__c]),
    'Calificaciones_areas_e'[REA__c] = "Comunicación"
)
```

---

### 3.7 Promedio_Matematica
**Descripción:** Promedio de calificaciones en Matemática.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
CALCULATE(
    AVERAGE('Calificaciones_areas_e'[Valor_Promedio__c]),
    'Calificaciones_areas_e'[REA__c] = "Matemática"
)
```

---

### 3.8 Promedio_Socioemocional
**Descripción:** Promedio de calificaciones en área Socioemocional.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
CALCULATE(
    AVERAGE('Calificaciones_areas_e'[Valor_Promedio__c]),
    'Calificaciones_areas_e'[REA__c] = "Sociemocional"
)
```

---

### 3.9 Promedio_Tecnologia
**Descripción:** Promedio de calificaciones en Tecnología.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
CALCULATE(
    AVERAGE('Calificaciones_areas_e'[Valor_Promedio__c]),
    'Calificaciones_areas_e'[REA__c] = "Tecnología"
)
```

---

### 3.10 numero_calificaciones_2025_I
**Descripción:** Número de niños calificados en el periodo 2025-I.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(DISTINCTCOUNT(Calificaciones_areas_e[DNI__c]),
          Calificaciones_areas_e[EVALUACION__c]="2025-I")
```

---

### 3.11 numero_calificaciones_2025_II
**Descripción:** Número de niños calificados en el periodo 2025-II.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(DISTINCTCOUNT(Calificaciones_areas_e[DNI__c]),
          Calificaciones_areas_e[EVALUACION__c]="2025-II")
```

---

### 3.12 Diferencia_Numero_Calificaciones
**Descripción:** Variación porcentual de calificaciones entre 2025-I y 2025-II.  
**Tipo de dato:** Decimal (Porcentaje)  
**Fórmula DAX:**
```dax
DIVIDE([numero_calificaciones_2025_II]-[numero_calificaciones_2025_I],
       [numero_calificaciones_2025_I])
```

---

### 3.13 niveles_competencia_c_2025-I
**Descripción:** Conteo de niveles de competencia alcanzados en 2025-I.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(COUNT(Calificaciones_areas_e[Competencia_texto__c]),
          Calificaciones_areas_e[EVALUACION__c]="2025-I")
```

---

### 3.14 niveles_alcanzado_c_2025-II
**Descripción:** Conteo de niveles de competencia alcanzados en 2025-II.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(COUNT(Calificaciones_areas_e[Competencia_texto__c]),
          Calificaciones_areas_e[EVALUACION__c]="2025-II")
```

---

### 3.15 Diferencia_Niveles_alcanzados
**Descripción:** Variación de niveles alcanzados entre 2025-I y 2025-II.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
DIVIDE([niveles_alcanzado_c_2025-II]-[niveles_competencia_c_2025-I],
       [niveles_competencia_c_2025-I])
```

---

### 3.16 Temas_por area
**Descripción:** Cuenta de temas únicos por área curricular seleccionada.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR AreaSeleccionada = SELECTEDVALUE('Progreso_curricular_e'[AREA__c])
RETURN
IF(
    NOT ISBLANK(AreaSeleccionada),
    CALCULATE(
        DISTINCTCOUNT('Progreso_curricular_e'[TEMAS__c]),
        'Progreso_curricular_e'[AREA__c] = AreaSeleccionada
    ),
    BLANK()
)
```

---

### 3.17 Etiqueta para progreso_curricular
**Descripción:** Etiqueta que muestra el progreso curricular con conteo y porcentaje.  
**Tipo de dato:** Texto  
**Fórmula DAX:**
```dax
VAR TotalCentro =
    CALCULATE(
        COUNTROWS('Progreso_curricular_e'),
        ALLEXCEPT(
            'Progreso_curricular_e',
            'Progreso_curricular_e'[CENTRO__c],
            'Progreso_curricular_e'[AREA__c],
            'Progreso_curricular_e'[STATUS__c],
            'Progreso_curricular_e'[CICLO__c],
            'Progreso_curricular_e'[BIMESTRE__c]
        )
    )
VAR Parte = COUNTROWS('Progreso_curricular_e')
VAR Porcentaje = DIVIDE(Parte, TotalCentro)
RETURN
Parte & " (" & FORMAT(Porcentaje, "0.0%") & ")"
```

---

## 4. Métricas de Satisfacción

### 4.1 SF_Niños_P1
**Descripción:** Promedio de respuesta a Pregunta 1 de satisfacción de niños.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
AVERAGE('Formulario_SatisNiños'[Pregunta_1])
```

---

### 4.2 SF_Niños_P2
**Descripción:** Promedio de respuesta a Pregunta 2 de satisfacción de niños.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
AVERAGE('Formulario_SatisNiños'[Pregunta_2])
```

---

### 4.3 SF_Niños_P3
**Descripción:** Promedio de respuesta a Pregunta 3 de satisfacción de niños.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
AVERAGE('Formulario_SatisNiños'[Pregunta_3])
```

---

### 4.4 SF_Niños_P4
**Descripción:** Promedio de respuesta a Pregunta 4 de satisfacción de niños.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
AVERAGE('Formulario_SatisNiños'[Pregunta_4])
```

---

### 4.5 SF_Niños_P5
**Descripción:** Promedio de respuesta a Pregunta 5 de satisfacción de niños.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
AVERAGE('Formulario_SatisNiños'[Pregunta_5])
```

---

### 4.6 Cantidad_padres que respondieron la encuesta
**Descripción:** Número de padres únicos que completaron la encuesta de satisfacción.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
DISTINCTCOUNT(Formulario_SatisfaccionInicial6grado_e[N_de_documento__c])
```

---

### 4.7 Cantidad__de hijos de padres que respondieron la encuesta
**Descripción:** Número de hijos de padres que completaron la encuesta.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
DISTINCTCOUNT(Formulario_SatisfaccionInicial6grado_e[N_de_documento_1__c])
```

---

### 4.8 Recuento_y_%_Mejora_Colegio
**Descripción:** Recuento y porcentaje de padres que consideran que su hijo mejoró en el colegio.  
**Tipo de dato:** Texto  
**Fórmula DAX:**
```dax
VAR Recuento =
    CALCULATE(
        COUNTROWS( Formulario_SatisfaccionInicial6grado_e )
    )
VAR TotalGeneral =
    CALCULATE(
        COUNTROWS( Formulario_SatisfaccionInicial6grado_e ),
        REMOVEFILTERS( Formulario_SatisfaccionInicial6grado_e[Considero_que_mi_hijo_mejor_en_el_coleg__c] )
    )
VAR Porcentaje =
    DIVIDE( Recuento, TotalGeneral )

RETURN
Recuento & " (" & FORMAT(Porcentaje, "0.0%") & ")"
```

---

### 4.9 Recuento_y_%_Aprendieron_Juego
**Descripción:** Recuento y porcentaje de padres que consideran que sus hijos aprendieron jugando.  
**Tipo de dato:** Texto  
**Fórmula DAX:**
```dax
VAR Recuento =
    CALCULATE(
        COUNTROWS( Formulario_SatisfaccionInicial6grado_e )
    )
VAR TotalGeneral =
    CALCULATE(
        COUNTROWS( Formulario_SatisfaccionInicial6grado_e ),
        REMOVEFILTERS( Formulario_SatisfaccionInicial6grado_e[Considero_que_mis_hijos_aprendieron__c] )
    )
VAR Porcentaje =
    DIVIDE( Recuento, TotalGeneral )

RETURN
Recuento & " (" & FORMAT(Porcentaje, "0.0%") & ")"
```

---

### 4.10 RespuestaWide
**Descripción:** Medida compleja que mapea respuestas de satisfacción según la pregunta seleccionada.  
**Tipo de dato:** Texto  
**Uso:** Permite crear visualizaciones dinámicas de encuestas de satisfacción.

---

### 4.11 NivelRespuesta
**Descripción:** Convierte respuestas textuales en niveles numéricos (1-4).  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
SWITCH (
    [RespuestaWide],
    "MUY EN DESACUERDO", 1,
    "EN DESACUERDO", 2,
    "DE ACUERDO", 3,
    "MUY DE ACUERDO", 4,
    BLANK ()
)
```

---

### 4.12 ConteoRespuestas
**Descripción:** Cuenta total de respuestas de satisfacción.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
COUNTROWS(Formulario_Satisfaccioninicial6grado_e)
```

---

### 4.13 LeyendaRespuesta
**Descripción:** Muestra la respuesta o "Sin respuesta" si está en blanco.  
**Tipo de dato:** Texto  
**Fórmula DAX:**
```dax
IF(
    ISBLANK([RespuestaWide]),
    "Sin respuesta",
    [RespuestaWide]
)
```

---

### 4.14 NivelUnificado
**Descripción:** Unifica diferentes escalas de respuesta (acuerdo, intensidad, frecuencia) en niveles 1-4.  
**Tipo de dato:** Entero  
**Uso:** Permite comparar resultados de preguntas con diferentes escalas.

---

### 4.15 ConteoPorNivel
**Descripción:** Cuenta de respuestas para un nivel específico seleccionado.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR _n = SELECTEDVALUE(TblLeyendaSatisfaccion[CategoriaOrden])
RETURN
CALCULATE(
    COUNTROWS(Formulario_Satisfaccioninicial6grado_e),
    FILTER(
        Formulario_Satisfaccioninicial6grado_e,
        [NivelUnificado] = _n
    )
)
```

---

### 4.16 Porcentaje_Indicador4_1
**Descripción:** Porcentaje de respuestas con nivel 1 en Indicador 4 (satisfacción de niños).  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
DIVIDE(
    CALCULATE(
        COUNTROWS(BD_Satisfaccion_Nino_e),
        BD_Satisfaccion_Nino_e[Indicador_4__c] = 1
    ),
    CALCULATE(
        COUNTROWS(BD_Satisfaccion_Nino_e),
        NOT ISBLANK(BD_Satisfaccion_Nino_e[Indicador_4__c])
    )
)
```

---

### 4.17 Porcentaje_Indicador4_3
**Descripción:** Porcentaje de respuestas con nivel 3 en Indicador 4.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
DIVIDE(
    CALCULATE(
        COUNTROWS(BD_Satisfaccion_Nino_e),
        BD_Satisfaccion_Nino_e[Indicador_4__c] = 3
    ),
    CALCULATE(
        COUNTROWS(BD_Satisfaccion_Nino_e),
        NOT ISBLANK(BD_Satisfaccion_Nino_e[Indicador_4__c])
    )
)
```

---

### 4.18 Porcentaje_Indicador4_4
**Descripción:** Porcentaje de respuestas con nivel 4 en Indicador 4.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
DIVIDE(
    CALCULATE(
        COUNTROWS(BD_Satisfaccion_Nino_e),
        BD_Satisfaccion_Nino_e[Indicador_4__c] = 4
    ),
    CALCULATE(
        COUNTROWS(BD_Satisfaccion_Nino_e),
        NOT ISBLANK(BD_Satisfaccion_Nino_e[Indicador_4__c])
    )
)
```

---

## 5. Métricas de Habilidades

### 5.1 Niños evaluados_por habilidades
**Descripción:** Cuenta de niños únicos evaluados en talleres de habilidades.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(
    DISTINCTCOUNT(Talleres_Habilidades[DNI])
)
```

---

### 5.2 %_Alumnos con nivel alcanzado_Habilidades
**Descripción:** Porcentaje de alumnos que alcanzaron un nivel específico en habilidades.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
VAR TotalAlumnos =
    CALCULATE(
        DISTINCTCOUNT(BD_Habilidades_e[DNI__c]),
        ALLSELECTED(BD_Habilidades_e[Nivel_de_logro__c])
    )
RETURN
DIVIDE(
    DISTINCTCOUNT(BD_Habilidades_e[DNI__c]),
    TotalAlumnos,
    0
)
```

---

### 5.3 Alumnos_únicos por nivel_Habilidades
**Descripción:** Cuenta de alumnos únicos por nivel de logro en habilidades.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
DISTINCTCOUNT(BD_Habilidades_e[DNI__c])
```

---

### 5.4 Niños_nivellogrado
**Descripción:** Cuenta de niños que alcanzaron el nivel "Logrado" en habilidades.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR Ninos =
    CALCULATE(
        COUNT(BD_Habilidades_e[DNI__c]),
        BD_Habilidades_e[Nivel_de_logro__c] = "Logrado"
    )
RETURN
COALESCE(Ninos, 0)
```

---

### 5.5 Niños_nivelsobresaliente
**Descripción:** Cuenta de niños que alcanzaron el nivel "Sobresaliente" en habilidades.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(
    COUNT(BD_Habilidades_e[DNI__c]),
    BD_Habilidades_e[Nivel_de_logro__c]="Sobresaliente"
)
```

---

### 5.6 Etiqueta_Porcentaje_y_Conteo_Habilidades
**Descripción:** Etiqueta formateada con porcentaje y conteo para habilidades.  
**Tipo de dato:** Texto  
**Fórmula DAX:**
```dax
VAR pct = [%_Alumnos con nivel alcanzado_Habilidades]
VAR cnt = [Alumnos_únicos por nivel_Habilidades]
RETURN
FORMAT(pct, "0.0%") & "  (" & FORMAT(cnt, "#,0") & " niños)"
```

---

### 5.7 Etiqueta_V2_Habilidades
**Descripción:** Versión alternativa de etiqueta para habilidades.  
**Tipo de dato:** Texto  
**Fórmula DAX:**
```dax
VAR NinosNivel =
    DISTINCTCOUNT(BD_Habilidades_e[DNI__c])

VAR TotalNinos =
    CALCULATE(
        DISTINCTCOUNT(BD_Habilidades_e[DNI__c]),
        ALL(BD_Habilidades_e[Nivel_de_logro__c])
    )

VAR Porcentaje =
    DIVIDE(NinosNivel, TotalNinos, 0)

RETURN
FORMAT(NinosNivel, "#,0") & " (" & FORMAT(Porcentaje, "0.0%") & ")"
```

---

### 5.8 Etiqueta_Nivel_Fix
**Descripción:** Etiqueta de nivel con corrección usando MAX en lugar de SELECTEDVALUE.  
**Tipo de dato:** Texto  
**Uso:** Soluciona problemas de ambigüedad en contextos de filtro múltiple.

---

### 5.9 Etiqueta_Numero_Porcentaje
**Descripción:** Etiqueta genérica que muestra conteo y porcentaje.  
**Tipo de dato:** Texto  
**Fórmula DAX:**
```dax
VAR NinosNivel =
    COUNT( BD_Habilidades_e[DNI__c] )

VAR TotalNinos =
    CALCULATE(
        COUNT( BD_Habilidades_e[DNI__c] ),
        REMOVEFILTERS( BD_Habilidades_e[Nivel_de_logro__c] )
    )

VAR Porcentaje =
    DIVIDE( NinosNivel, TotalNinos, 0 )

RETURN
FORMAT( NinosNivel, "#,0" ) & " (" & FORMAT( Porcentaje, "0.0%" ) & ")"
```

---

## 6. Métricas de Talleres y Actividades

### 6.1 Cantidad_talleresextracurriculares
**Descripción:** Número de talleres extracurriculares realizados.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
CALCULATE(
    DISTINCTCOUNT(Asistencias_Extracurriculares_e[FECHA__c]))
```

---

### 6.2 Numero_beneficiarios_talleres_vivenciales
**Descripción:** Número fijo de beneficiarios de talleres vivenciales.  
**Tipo de dato:** Entero  
**Valor:** 2017

---

### 6.3 Numero_experiencias_vivenciales
**Descripción:** Número fijo de experiencias vivenciales realizadas.  
**Tipo de dato:** Entero  
**Valor:** 57

---

### 6.4 Horas_Aprendizaje
**Descripción:** Total de horas de aprendizaje distribuidas.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
SUM('BD_DistribucionHoraria__e'[Valor])
```
**Formato:** #,0

---

### 6.5 Horas_Aprendizaje_Filtrada
**Descripción:** Horas de aprendizaje filtradas por centro seleccionado.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR FiltroCentro = SELECTEDVALUE(Asistencias_e7[Centro_Limpio])

VAR TotalGeneral =
    SUM(BD_DistribucionHoraria__e[Valor])

VAR TotalFiltrado =
    CALCULATE(
        SUM(BD_DistribucionHoraria__e[Valor]),
        TREATAS( VALUES(Asistencias_e7[Centro_Limpio]), BD_DistribucionHoraria__e[Nombre_Limpio] )
    )

RETURN
IF(
    ISBLANK(FiltroCentro),
    TotalGeneral,
    TotalFiltrado
)
```

---

## 7. Métricas de Impacto y Variación

### 7.1 Niños_activos_Acumulada Corte 1
**Descripción:** Niños activos acumulados hasta el corte 1.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR Corte = SELECTEDVALUE(CalendarioFiltro1[Date])
RETURN
CALCULATE(
    DISTINCTCOUNT(Asistencias_e7[DNI__c]),
    Asistencias_e7[TIPO_ALUMNO__c] IN {"ACTIVO"},
    Asistencias_e7[FECHA__c] <= Corte,
    ALL(Asistencias_e7[FECHA__c])
)
```

---

### 7.2 Niños_inactivos_Acumulada Corte 1
**Descripción:** Niños inactivos acumulados hasta el corte 1.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR Corte = SELECTEDVALUE(CalendarioFiltro1[Date])
RETURN
CALCULATE(
    DISTINCTCOUNT(Asistencias_e7[DNI__c]),
    Asistencias_e7[TIPO_ALUMNO__c] IN {"INACTIVO"},
    Asistencias_e7[FECHA__c] <= Corte,
    ALL(Asistencias_e7[FECHA__c])
)
```

---

### 7.3 Niños_impactados_Acumulada Corte 1
**Descripción:** Total de niños impactados (activos + inactivos) hasta el corte 1.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR Corte = SELECTEDVALUE(CalendarioFiltro1[Date])
RETURN
CALCULATE(
    DISTINCTCOUNT(Asistencias_e7[DNI__c]),
    Asistencias_e7[TIPO_ALUMNO__c] IN {"INACTIVO", "ACTIVO"},
    Asistencias_e7[FECHA__c] <= Corte,
    ALL(Asistencias_e7[FECHA__c])
)
```

---

### 7.4 Niños_activos_Acumulada Corte 2
**Descripción:** Niños activos acumulados hasta el corte 2.  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
VAR Corte = SELECTEDVALUE(CalendarioFiltro2[Date])
RETURN
CALCULATE(
    DISTINCTCOUNT(Asistencias_e7[DNI__c]),
    Asistencias_e7[TIPO_ALUMNO__c] IN {"ACTIVO"},
    Asistencias_e7[FECHA__c] <= Corte,
    ALL(Asistencias_e7[FECHA__c])
)
```

---

### 7.5 Niños_inactivos_Acumulada Corte 2
**Descripción:** Niños inactivos acumulados hasta el corte 2.  
**Tipo de dato:** Entero

---

### 7.6 Niños_impactados_Acumulada Corte 2
**Descripción:** Total de niños impactados hasta el corte 2.  
**Tipo de dato:** Entero

---

### 7.7 Niños_activos_Acumulada_E Corte 3
**Descripción:** Niños activos en extracurriculares acumulados hasta el corte 3.  
**Tipo de dato:** Entero

---

### 7.8 Niños_inactivos_Acumulada_E Corte 3
**Descripción:** Niños inactivos en extracurriculares acumulados hasta el corte 3.  
**Tipo de dato:** Entero

---

### 7.9 Niños_impactados_Acumulada_E Corte 3
**Descripción:** Total de niños impactados en extracurriculares hasta el corte 3.  
**Tipo de dato:** Entero

---

### 7.10 Niños_activos_Acumulada_E Corte 4
**Descripción:** Niños activos en extracurriculares acumulados hasta el corte 4.  
**Tipo de dato:** Entero

---

### 7.11 Niños_inactivos_Acumulada_E Corte 4
**Descripción:** Niños inactivos en extracurriculares acumulados hasta el corte 4.  
**Tipo de dato:** Entero

---

### 7.12 Niños_impactados_Acumulada_E Corte 4
**Descripción:** Total de niños impactados en extracurriculares hasta el corte 4.  
**Tipo de dato:** Entero

---

### 7.13 Corte1_Valor
**Descripción:** Selector de métrica para el corte 1 (impactados/activos/inactivos).  
**Tipo de dato:** Entero  
**Fórmula DAX:**
```dax
SWITCH(
    SELECTEDVALUE(TablaCuadro[Fila]),
    "N° de niños(as) impactados", [Niños_impactados_Acumulada Corte 1],
    "N° niños activos",           [Niños_activos_Acumulada Corte 1],
    "N° niños inactivos",         [Niños_inactivos_Acumulada Corte 1]
)
```

---

### 7.14 Corte2_Valor
**Descripción:** Selector de métrica para el corte 2.  
**Tipo de dato:** Entero

---

### 7.15 Corte3_Valor
**Descripción:** Selector de métrica para el corte 3 (extracurriculares).  
**Tipo de dato:** Entero

---

### 7.16 Corte4_Valor
**Descripción:** Selector de métrica para el corte 4 (extracurriculares).  
**Tipo de dato:** Entero

---

### 7.17 Variacion_corte12
**Descripción:** Variación porcentual entre corte 1 y corte 2.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
DIVIDE([Corte2_Valor] - [Corte1_Valor], [Corte1_Valor])
```

---

### 7.18 Variacion_corte34
**Descripción:** Variación porcentual entre corte 3 y corte 4 (extracurriculares).  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
DIVIDE([Corte4_Valor] - [Corte3_Valor], [Corte3_Valor])
```

---

### 7.19 Porcentaje_de variacion entre los cortes de asistencia
**Descripción:** Variación porcentual de asistencias entre corte 1 y 2.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
DIVIDE(
    [Asistencia_Acumulada Corte 2] - [Asistencia_Acumulada Corte 1],
    [Asistencia_Acumulada Corte 1]
)
```

---

### 7.20 Porcentaje_de variacion entre los cortes de asistencia ACT_Extra
**Descripción:** Variación porcentual de asistencias extracurriculares entre corte 3 y 4.  
**Tipo de dato:** Decimal  
**Fórmula DAX:**
```dax
DIVIDE(
    [Asistencia_Acumulada Corte 4] - [Asistencia_Acumulada Corte 3],
    [Asistencia_Acumulada Corte 3]
)
```

---

## 8. Métricas de Visualización y Formato

### 8.1 Etiqueta_Porcentaje_y_Conteo
**Descripción:** Etiqueta formateada que combina porcentaje y conteo de niños.  
**Tipo de dato:** Texto  
**Fórmula DAX:**
```dax
VAR pct = [%_Alumnos con nivel alcanzado]
VAR cnt = [Alumnos_únicos por nivel]
RETURN
FORMAT(pct, "0.0%") & "  (" & FORMAT(cnt, "#,0") & " niños)"
```
**Uso:** Etiquetas de datos en visualizaciones.

---

### 8.2 Caritas_Satisfaccion_HTML
**Descripción:** Genera código HTML con caritas de satisfacción (emojis SVG).  
**Tipo de dato:** Texto  
**Uso:** Visualización gráfica de escala de satisfacción en reportes HTML.

---

### 8.3 Caritas_HTML
**Descripción:** Genera código HTML con caritas de satisfacción con relleno progresivo según nivel.  
**Tipo de dato:** Texto  
**Uso:** Visualización avanzada de niveles de satisfacción.

---

### 8.4 Caracteres_Pachacutec
**Descripción:** Utilidad para extraer caracteres específicos (prueba técnica).  
**Tipo de dato:** Texto  
**Fórmula DAX:**
```dax
UNICHAR(UNICODE(MID("PACHACÚTEC", 7, 1)))
```

---

### 8.5 Nivel_alcanzado_2025_I
**Descripción:** Medida en estado de error - contiene referencia a columna inexistente.  
**Estado:** SemanticError  
**Nota:** Requiere corrección antes de uso.

---

## Resumen Ejecutivo

### KPIs Principales

| Categoría | Medida Principal | Valor Esperado | Uso |
|-----------|------------------|----------------|-----|
| **Cobertura** | Niños_Kantaya | > 1000 | Total de beneficiarios |
| **Retención** | Porcentaje_de retencion | > 85% | Tasa de permanencia |
| **Asistencia** | Promedio_Asistencia_Global | > 80% | Participación activa |
| **Logro Académico** | %_Alumnos con nivel alcanzado | > 70% | Niveles de competencia |
| **Satisfacción** | SF_Niños_P1-P5 | > 3.5/5 | Percepción de niños |
| **Habilidades** | Niños_nivellogrado + Niños_nivelsobresaliente | > 60% | Desarrollo de habilidades |

---

### Categorías de Medidas

1. **Asistencia (11 medidas):** Seguimiento de participación en actividades regulares y extracurriculares
2. **Niños y Participantes (16 medidas):** Cobertura, retención y deserción
3. **Evaluación y Calificaciones (17 medidas):** Desempeño académico por áreas
4. **Satisfacción (18 medidas):** Percepción de niños y padres sobre el programa
5. **Habilidades (9 medidas):** Evaluación de competencias socioemocionales y técnicas
6. **Talleres y Actividades (6 medidas):** Oferta y participación en actividades complementarias
7. **Impacto y Variación (20 medidas):** Análisis de tendencias y cambios temporales
8. **Visualización (5 medidas):** Herramientas de formato y presentación

---

### Recomendaciones de Uso

**Para Reportes Ejecutivos:**
- Niños_Kantaya
- Porcentaje_de retencion
- Promedio_Asistencia_Global
- %_Alumnos con nivel alcanzado
- SF_Niños (promedio de P1-P5)

**Para Análisis de Tendencias:**
- Variacion_corte12 / Variacion_corte34
- Diferencia_Numero_Calificaciones
- Diferencia_Niveles_alcanzados
- Porcentaje_de variacion entre los cortes de asistencia

**Para Seguimiento Operativo:**
- Asistencias_acumuladas
- Numero_de clases
- Cantidad_talleresextracurriculares
- Nuevos_activos_mes
- Nuevos_inactivos_mes

---

### Alertas y Medidas con Error

**Medidas que requieren revisión:**
- `Nivel_alcanzado_2025_I`: Estado SemanticError - Columna 'Ev' no existe en tabla Asistencias_e7

---

**Fin del documento**
