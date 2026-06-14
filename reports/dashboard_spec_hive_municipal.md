# Especificacion Power BI: 6 Dashboards Municipales Desde Hive

## Fuente De Datos

Power BI debe consumir la base Hive `municipal_gold`, conectandose a
HiveServer2 (`localhost:10000`) por ODBC. Para evitar errores de metadata del
driver, importar las tablas materializadas `pbi_dashboard_01` a
`pbi_dashboard_06`.

Todas las paginas usan `categoria_municipalidad` como segmentador principal,
derivado de `CategoriasMunicipalidades.csv`.

Para indicadores ejecutivos generales tambien se puede cargar
`pbi_kpi_resumen_ejecutivo`, que materializa el mart Gold de KPIs en Parquet.

## Modelo

El modelo analitico final es una constelacion de hechos en Gold:

- Dimensiones: municipalidad, ubigeo, tiempo, clasificador, estado SISMEPRE,
  formulario y pregunta.
- Hechos: ingresos, clasificador, predial, cumplimiento SISMEPRE, RENAMU
  gestion tributaria y RENAMU software.
- Mart: `mart_dashboard_municipal` para priorizacion.
- Mart KPI: `mart_kpi_resumen_ejecutivo` para tarjetas ejecutivas y validacion
  de indicadores transversales.

## Estilo Visual

- Encabezado verde oscuro.
- Fondo verde claro.
- Paneles blancos para graficos.
- KPIs grandes en la parte superior.
- Segmentador lateral por categoria `A-G`.
- Tablas con cabecera verde.
- Sin mapas externos: solo departamento, provincia, distrito y UBIGEO.

## Pagina 1: Recaudacion Municipal Vs Capacidad Tributaria

Tabla Power BI: `pbi_dashboard_01`

Objetivo: comparar la recaudacion SIAF con la capacidad administrativa RENAMU.

Visuales:

- KPI `recaudacion_total`.
- KPI `personal_municipal_total`.
- KPI `recaudacion_por_personal`.
- Barras: recaudacion por `DEPARTAMENTO_NOMBRE`.
- Scatter: `personal_municipal_total` vs `recaudacion_total`.

Filtros:

- `year`
- `categoria_municipalidad`
- `DEPARTAMENTO_NOMBRE`

## Pagina 2: Recaudacion Por Clasificador De Ingreso

Tabla Power BI: `pbi_dashboard_02`

Objetivo: identificar de donde provienen los ingresos municipales.

Visuales:

- KPI `pia`.
- KPI `pim`.
- KPI `recaudado`.
- Barras: `ESPECIFICA_NOMBRE` por recaudado.
- Tabla: departamento, provincia, distrito, rubro, especifica, recaudado.

Filtros:

- `year`
- `categoria_municipalidad`
- `RUBRO_NOMBRE`

## Pagina 3: Predial Vs Efectividad

Tabla Power BI: `pbi_dashboard_03`

Objetivo: ver si mayor emision predial se traduce en recaudacion efectiva.

Visuales:

- KPI `recaudacion_predial_total`.
- KPI `emision_predial_total`.
- KPI `efectividad_predial_pct`.
- Scatter: `efectividad_predial_pct` vs `recaudacion_predial_total`.
- Barras: recaudacion predial por departamento.

Filtros:

- `year`
- `categoria_municipalidad`
- `tipo_meta`

## Pagina 4: Distribucion De Efectividad Predial

Tabla Power BI: `pbi_dashboard_04`

Objetivo: clasificar municipios por desempeno predial.

Visuales:

- Barras: efectividad promedio por departamento.
- Histograma: `efectividad_predial_pct`.
- Tabla ranking: departamento, distrito, categoria, efectividad, ranking.

Filtros:

- `year`
- `categoria_municipalidad`
- `DEPARTAMENTO_NOMBRE`

## Pagina 5: Software Tributario Municipal

Tabla Power BI: `pbi_dashboard_05`

Objetivo: evaluar capacidades tecnologicas de administracion tributaria.

Visuales:

- KPI cantidad con SRTM.
- KPI cantidad con software propio de rentas.
- KPI cantidad con software de catastro.
- Barras apiladas por departamento: `usa_srtm_estado`.
- Barras apiladas por departamento: `usa_software_rentas_at`.
- Barras apiladas por departamento: `usa_software_catastro`.

Filtros:

- `categoria_municipalidad`
- `DEPARTAMENTO_NOMBRE`

## Pagina 6: Priorizacion De Municipalidades

Tabla Power BI: `pbi_dashboard_06`

Objetivo: decidir que municipalidades requieren apoyo o fiscalizacion primero.

Visuales:

- KPI saldo predial total.
- KPI recaudacion total.
- KPI efectividad predial.
- Dona o gauge: porcentaje de efectividad predial.
- Tabla priorizada: departamento, provincia, distrito, categoria, recaudacion,
  saldo predial, efectividad, software, estado SISMEPRE y prioridad.

Filtros:

- `year`
- `categoria_municipalidad`
- `prioridad_intervencion`

## Medidas DAX Recomendadas

```DAX
Recaudacion Total = SUM('01_recaudacion_capacidad'[recaudacion_total])
PIM Total = SUM('02_clasificador_ingreso'[pim])
Pct Ejecucion = DIVIDE([Recaudacion Total], [PIM Total])
Personal Municipal = SUM('01_recaudacion_capacidad'[personal_municipal_total])
Recaudacion Por Personal = DIVIDE([Recaudacion Total], [Personal Municipal])
Recaudacion Predial = SUM('03_predial_vs_efectividad'[recaudacion_predial_total])
Emision Predial = SUM('03_predial_vs_efectividad'[emision_predial_total])
Efectividad Predial = DIVIDE([Recaudacion Predial], [Emision Predial])
KPI Ejecucion Recaudacion = AVERAGE('kpi_resumen_ejecutivo'[kpi_pct_ejecucion_recaudacion])
KPI Variacion PIM PIA = SUM('kpi_resumen_ejecutivo'[kpi_variacion_pim_pia])
KPI Capacidad Software = AVERAGE('kpi_resumen_ejecutivo'[kpi_capacidad_software_pct])
```

## KPIs Ejecutivos De Apoyo

Tabla opcional: `pbi_kpi_resumen_ejecutivo`

Usala para tarjetas generales, validaciones o una pagina de apoyo tecnica, no
como reemplazo de las seis paginas principales.

- `% ejecucion de recaudacion`: avance de ingresos contra PIM.
- `variacion PIM - PIA`: cambios presupuestarios.
- `efectividad predial`: cobranza predial contra emision.
- `capacidad software tributario`: cobertura SRTM, rentas y catastro.

## Entrega Recomendada

1. Ejecutar Bronze, Silver y Gold.
2. Ejecutar `scripts/hive_bootstrap.py`.
3. Ejecutar `scripts/materialize_powerbi_hive_tables.py` para crear
   `pbi_dashboard_01..06` y `pbi_kpi_resumen_ejecutivo`.
4. Ejecutar `scripts/export_powerbi_from_hive.py` si se desea workbook Excel.
5. En Power BI Desktop, importar `powerbi_municipal_hive.xlsx` o conectar a
   HiveServer2.
6. Crear las seis paginas con los visuales anteriores.
