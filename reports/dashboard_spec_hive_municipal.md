# Especificacion Power BI: 6 Dashboards Municipales Desde Hive

## Fuente De Datos

Power BI debe consumir la base Hive `municipal_gold`, ya sea conectandose a
HiveServer2 (`localhost:10000`) o importando el workbook generado desde las
vistas Hive.

Todas las paginas usan `categoria_municipalidad` como segmentador principal,
derivado de `CategoriasMunicipalidades.csv`.

## Modelo

El modelo analitico final es una constelacion de hechos en Gold:

- Dimensiones: municipalidad, ubigeo, tiempo, clasificador, estado SISMEPRE,
  formulario y pregunta.
- Hechos: ingresos, clasificador, predial, cumplimiento SISMEPRE, RENAMU
  gestion tributaria y RENAMU software.
- Mart: `mart_dashboard_municipal` para priorizacion.

## Estilo Visual

- Encabezado verde oscuro.
- Fondo verde claro.
- Paneles blancos para graficos.
- KPIs grandes en la parte superior.
- Segmentador lateral por categoria `A-G`.
- Tablas con cabecera verde.
- Sin mapas externos: solo departamento, provincia, distrito y UBIGEO.

## Pagina 1: Recaudacion Municipal Vs Capacidad Tributaria

Vista Hive: `vw_dashboard_01_recaudacion_capacidad`

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

Vista Hive: `vw_dashboard_02_clasificador_ingreso`

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

Vista Hive: `vw_dashboard_03_predial_vs_efectividad`

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

Vista Hive: `vw_dashboard_04_distribucion_efectividad`

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

Vista Hive: `vw_dashboard_05_software_tributario`

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

Vista Hive: `vw_dashboard_06_priorizacion_municipal`

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
```

## Entrega Recomendada

1. Ejecutar Bronze, Silver y Gold.
2. Ejecutar `scripts/hive_bootstrap.py`.
3. Ejecutar `scripts/export_powerbi_from_hive.py`.
4. En Power BI Desktop, importar `powerbi_municipal_hive.xlsx` o conectar a
   HiveServer2.
5. Crear las seis paginas con los visuales anteriores.
