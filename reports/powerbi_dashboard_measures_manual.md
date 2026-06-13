# Medidas DAX Y Diseño Manual Para Los 6 Dashboards Power BI

Este archivo usa los nombres reales detectados en el PBIX `Dashboard_Municipal_Gold_Hive_2`.
Actualmente las columnas quedaron con prefijo `vw_dashboard_*` porque se cargaron desde vistas Hive. Si luego cambias las consultas a `pbi_dashboard_01`...`pbi_dashboard_06` y Power BI cambia los nombres de columnas, solo reemplaza el prefijo en las medidas.

## Tablas Que Debes Usar

Para los dashboards carga estas tablas desde Hive:

```powerquery
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_01")
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_02")
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_03")
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_04")
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_05")
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_06")
```

No cargues `vw_dashboard_*` para el dashboard final. Esas vistas sirven como evidencia SQL, pero las tablas `pbi_dashboard_*` son más estables para Power BI.

## Categorías A-G

La columna `categoria_municipalidad` viene del archivo `CategoriasMunicipalidades.csv` entregado por el profesor. Se usa como filtro y segmentador de análisis.

En Power BI úsala como panel lateral tipo botones:

- `A`
- `B`
- `C`
- `D`
- `E`
- `F`
- `G`
- `(En blanco)` para municipalidades sin match confiable por nombre.

No inventar significado oficial de A-G si el profesor no entregó diccionario. En la exposición di: "La categoría A-G proviene del archivo adicional del docente y se usa para segmentar municipalidades".

## Medidas Generales

### Dashboard 1: Recaudación Municipal Vs Capacidad Tributaria

Tabla: `01_recaudacion_capacidad`

```DAX
Recaudacion Total =
SUM('01_recaudacion_capacidad'[vw_dashboard_01_recaudacion_capacidad.recaudacion_total])
```

```DAX
PIM Total Capacidad =
SUM('01_recaudacion_capacidad'[vw_dashboard_01_recaudacion_capacidad.pim_total])
```

```DAX
Personal Municipal =
SUM('01_recaudacion_capacidad'[vw_dashboard_01_recaudacion_capacidad.personal_municipal_total])
```

```DAX
Recaudacion Por Personal =
DIVIDE([Recaudacion Total], [Personal Municipal])
```

```DAX
Municipalidades Capacidad =
DISTINCTCOUNT('01_recaudacion_capacidad'[vw_dashboard_01_recaudacion_capacidad.sec_ejec])
```

```DAX
Pct Ejecucion Capacidad =
DIVIDE([Recaudacion Total], [PIM Total Capacidad])
```

Visuales recomendados:

- Tarjetas: `Recaudacion Total`, `Personal Municipal`, `Recaudacion Por Personal`.
- Barras: eje `departamento_nombre`, valor `Personal Municipal` o `Recaudacion Total`.
- Dispersión: X `personal_municipal_total`, Y `recaudacion_total`, leyenda `categoria_municipalidad`.
- Filtros: `year`, `categoria_municipalidad`, `departamento_nombre`.

### Dashboard 2: Recaudación Por Clasificador De Ingreso

Tabla: `02_clasificador_ingreso`

```DAX
PIA Total =
SUM('02_clasificador_ingreso'[vw_dashboard_02_clasificador_ingreso.pia])
```

```DAX
PIM Total =
SUM('02_clasificador_ingreso'[vw_dashboard_02_clasificador_ingreso.pim])
```

```DAX
Recaudado Total =
SUM('02_clasificador_ingreso'[vw_dashboard_02_clasificador_ingreso.recaudado])
```

```DAX
Pct Ejecucion =
DIVIDE([Recaudado Total], [PIM Total])
```

```DAX
Variacion PIM PIA =
[PIM Total] - [PIA Total]
```

```DAX
Municipalidades Clasificador =
DISTINCTCOUNT('02_clasificador_ingreso'[vw_dashboard_02_clasificador_ingreso.sec_ejec])
```

Visuales recomendados:

- Tarjetas: `PIA Total`, `PIM Total`, `Recaudado Total`, `Pct Ejecucion`.
- Barras: eje `especifica_nombre`, valor `Recaudado Total`.
- Tabla: `departamento_nombre`, `provincia_nombre`, `distrito_nombre`, `Recaudado Total`.
- Filtros: `year`, `month`, `categoria_municipalidad`, `rubro_nombre`.

### Dashboard 3: Predial Vs Efectividad

Tabla: `03_predial_vs_efectividad`

```DAX
Recaudacion Predial =
SUM('03_predial_vs_efectividad'[vw_dashboard_03_predial_vs_efectividad.recaudacion_predial_total])
```

```DAX
Emision Predial =
SUM('03_predial_vs_efectividad'[vw_dashboard_03_predial_vs_efectividad.emision_predial_total])
```

```DAX
Efectividad Predial =
DIVIDE([Recaudacion Predial], [Emision Predial])
```

```DAX
Brecha Predial =
[Emision Predial] - [Recaudacion Predial]
```

```DAX
Municipalidades Predial =
DISTINCTCOUNT('03_predial_vs_efectividad'[vw_dashboard_03_predial_vs_efectividad.sec_ejec])
```

Visuales recomendados:

- Tarjetas: `Recaudacion Predial`, `Emision Predial`, `Efectividad Predial`, `Brecha Predial`.
- Barras: eje `departamento_nombre`, valor `Recaudacion Predial`.
- Dispersión: X `efectividad_predial_pct`, Y `recaudacion_predial_total`, leyenda `categoria_municipalidad`.
- Filtros: `year`, `categoria_municipalidad`, `tipo_meta`.

### Dashboard 4: Distribución De Efectividad Predial

Tabla: `04_distribucion_efectividad`

```DAX
Efectividad Predial Promedio =
AVERAGE('04_distribucion_efectividad'[vw_dashboard_04_distribucion_efectividad.efectividad_predial_pct]) / 100
```

```DAX
Recaudacion Predial Dist =
SUM('04_distribucion_efectividad'[vw_dashboard_04_distribucion_efectividad.recaudacion_predial_total])
```

```DAX
Emision Predial Dist =
SUM('04_distribucion_efectividad'[vw_dashboard_04_distribucion_efectividad.emision_predial_total])
```

```DAX
Municipalidades Distribucion =
DISTINCTCOUNT('04_distribucion_efectividad'[vw_dashboard_04_distribucion_efectividad.sec_ejec])
```

Visuales recomendados:

- Tarjetas: `Efectividad Predial Promedio`, `Recaudacion Predial Dist`, `Municipalidades Distribucion`.
- Barras: eje `departamento_nombre`, valor `Efectividad Predial Promedio`.
- Histograma: eje `decil_efectividad`, valor conteo de `sec_ejec`.
- Filtros: `year`, `categoria_municipalidad`, `departamento_nombre`.

### Dashboard 5: Software Tributario Municipal

Tabla: `05_software_tributario`

Estas medidas corrigen el error de comparar texto `SI/NO` contra booleanos.

```DAX
Municipalidades Software =
DISTINCTCOUNT('05_software_tributario'[vw_dashboard_05_software_tributario.sec_ejec])
```

```DAX
Municipalidades Con Software AT =
COUNTROWS(
    FILTER(
        '05_software_tributario',
        UPPER('05_software_tributario'[vw_dashboard_05_software_tributario.usa_al_menos_un_software_at]) = "SI"
    )
)
```

```DAX
Municipalidades Con SRTM =
COUNTROWS(
    FILTER(
        '05_software_tributario',
        UPPER('05_software_tributario'[vw_dashboard_05_software_tributario.usa_srtm_estado]) = "SI"
    )
)
```

```DAX
Municipalidades Con Software Rentas =
COUNTROWS(
    FILTER(
        '05_software_tributario',
        UPPER('05_software_tributario'[vw_dashboard_05_software_tributario.usa_software_rentas_at]) = "SI"
    )
)
```

```DAX
Municipalidades Con Software Catastro =
COUNTROWS(
    FILTER(
        '05_software_tributario',
        UPPER('05_software_tributario'[vw_dashboard_05_software_tributario.usa_software_catastro]) = "SI"
    )
)
```

```DAX
Pct Con Software AT =
DIVIDE([Municipalidades Con Software AT], [Municipalidades Software])
```

Visuales recomendados:

- Tarjetas: `Municipalidades Con SRTM`, `Municipalidades Con Software Rentas`, `Municipalidades Con Software Catastro`, `Pct Con Software AT`.
- Barras apiladas: eje `departamento_nombre`, leyenda `usa_srtm_estado`, valor conteo de `sec_ejec`.
- Barras apiladas: eje `departamento_nombre`, leyenda `usa_software_rentas_at`.
- Barras apiladas: eje `departamento_nombre`, leyenda `usa_software_catastro`.
- Filtros: `categoria_municipalidad`, `departamento_nombre`, `year`.

### Dashboard 6: Priorización De Municipalidades

Tabla: `06_priorizacion_municipal`

```DAX
Recaudacion Total Priorizacion =
SUM('06_priorizacion_municipal'[vw_dashboard_06_priorizacion_municipal.recaudacion_total])
```

```DAX
Saldo Predial Total =
SUM('06_priorizacion_municipal'[vw_dashboard_06_priorizacion_municipal.saldo_predial_total])
```

```DAX
Recaudacion Predial Priorizacion =
SUM('06_priorizacion_municipal'[vw_dashboard_06_priorizacion_municipal.recaudacion_predial_total])
```

```DAX
Base Imponible Predial =
SUM('06_priorizacion_municipal'[vw_dashboard_06_priorizacion_municipal.base_imponible_predial])
```

```DAX
Efectividad Priorizacion Promedio =
AVERAGE('06_priorizacion_municipal'[vw_dashboard_06_priorizacion_municipal.efectividad_predial_pct]) / 100
```

```DAX
Municipalidades Alta Prioridad =
COUNTROWS(
    FILTER(
        '06_priorizacion_municipal',
        UPPER('06_priorizacion_municipal'[vw_dashboard_06_priorizacion_municipal.prioridad_intervencion]) = "ALTA"
    )
)
```

```DAX
Municipalidades Priorizacion =
DISTINCTCOUNT('06_priorizacion_municipal'[vw_dashboard_06_priorizacion_municipal.sec_ejec])
```

```DAX
Pct Alta Prioridad =
DIVIDE([Municipalidades Alta Prioridad], [Municipalidades Priorizacion])
```

Visuales recomendados:

- Tarjetas: `Base Imponible Predial`, `Recaudacion Predial Priorizacion`, `Saldo Predial Total`, `Pct Alta Prioridad`.
- Medidor: `Efectividad Priorizacion Promedio`.
- Tabla principal: `departamento_nombre`, `provincia_nombre`, `distrito_nombre`, `categoria_municipalidad`, `recaudacion_total`, `saldo_predial_total`, `efectividad_predial_pct`, `usa_al_menos_un_software_at`, `estado_sismepre`, `prioridad_intervencion`.
- Filtros: `year`, `categoria_municipalidad`, `prioridad_intervencion`, `departamento_nombre`.

## Estilo Visual Similar Al Ejemplo

Usa este patrón en las 6 páginas:

- Encabezado superior verde: `#12A72D`.
- Fondo verde claro: `#DDF8D8`.
- Paneles de gráficos blancos: `#F8FFF7`.
- Barras principales verdes: `#43B80F`.
- Alertas o brecha: naranja `#FF8A00` o rojo suave `#E05D6F`.
- Categorías A-G como slicer vertical a la izquierda.
- Años como slicer horizontal arriba.
- Tarjetas KPI grandes en la parte superior.

## Orden De Páginas

1. Recaudación Municipal Vs Capacidad Tributaria.
2. Recaudación Por Clasificador De Ingreso.
3. Predial Vs Efectividad.
4. Distribución De Efectividad Predial.
5. Software Tributario Municipal.
6. Priorización De Municipalidades.

