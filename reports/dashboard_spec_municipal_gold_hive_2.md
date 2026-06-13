# Especificacion de dashboards Power BI - Municipal Gold Hive

Archivo Power BI:

```text
Dashboard_Municipal_Gold_Hive_2.pbix
```

Origen de datos:

```powerquery
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_01")
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_02")
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_03")
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_04")
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_05")
= Odbc.Query("dsn=MunicipalHive", "SELECT * FROM municipal_gold.pbi_dashboard_06")
```

Estilo visual recomendado:

- Encabezado verde: `#12A72D`
- Fondo verde claro: `#DDF8D8`
- Paneles de visuales: blanco o `#F8FFF7`
- Barras principales: `#43B80F`
- Alertas/brechas: naranja `#FF8A00` o rojo suave `#E05D6F`
- Slicer lateral de categoria A-G en todas las paginas
- Slicer horizontal de anio en la parte superior

Nota sobre categoria:

`categoria_municipalidad` viene del archivo `CategoriasMunicipalidades.csv` entregado por el docente. Se usa como clasificacion A-G para segmentar municipalidades. No inventar significado especifico de cada letra si no existe diccionario oficial.

## Pagina 1: Recaudacion municipal vs capacidad tributaria

Tabla:

```text
01_recaudacion_capacidad
```

Objetivo:

Comparar la recaudacion municipal del SIAF con la capacidad operativa aproximada de RENAMU, usando personal municipal y categoria A-G.

Visuales:

- Cards:
  - `Recaudacion Total`
  - `Personal Municipal`
  - `Recaudacion Por Personal`
  - `Pct Ejecucion Capacidad`
- Barras horizontales:
  - Eje: `pbi_dashboard_01.departamento_nombre`
  - Valor: `Personal Municipal`
  - Orden: descendente
  - Top N opcional: 20 departamentos/territorios
- Barras horizontales alternativa:
  - Eje: `pbi_dashboard_01.departamento_nombre`
  - Valor: `Recaudacion Total`
- Scatter:
  - X: `pbi_dashboard_01.personal_municipal_total`
  - Y: `pbi_dashboard_01.recaudacion_total`
  - Size: `pbi_dashboard_01.recaudacion_total`
  - Legend: `pbi_dashboard_01.categoria_municipalidad`
  - Details: `pbi_dashboard_01.municipalidad_nombre`
  - Tooltips:
    - `pbi_dashboard_01.ubigeo`
    - `pbi_dashboard_01.departamento_nombre`
    - `pbi_dashboard_01.provincia_nombre`
    - `pbi_dashboard_01.distrito_nombre`
    - `Recaudacion Por Personal`

Filtros:

- `pbi_dashboard_01.year`
- `pbi_dashboard_01.categoria_municipalidad`
- `pbi_dashboard_01.departamento_nombre`
- `pbi_dashboard_01.requiere_asistencia_at`

Titulo sugerido:

```text
Peru. Analisis comparativo entre la recaudacion municipal y la capacidad tributaria.
```

## Pagina 2: Recaudacion por clasificador de ingreso

Tabla:

```text
02_clasificador_ingreso
```

Objetivo:

Identificar los rubros, genericas, subgenericas y especificas que explican la recaudacion municipal.

Visuales:

- Cards:
  - `PIA Total`
  - `PIM Total`
  - `Recaudado Total`
  - `Pct Ejecucion`
  - `Variacion PIM PIA`
- Barras horizontales:
  - Eje: `pbi_dashboard_02.especifica_nombre`
  - Valor: `Recaudado Total`
  - Top N: 20
  - Orden: descendente
- Barras o treemap:
  - Categoria: `pbi_dashboard_02.rubro_nombre`
  - Valor: `Recaudado Total`
- Tabla territorial:
  - `pbi_dashboard_02.departamento_nombre`
  - `pbi_dashboard_02.provincia_nombre`
  - `pbi_dashboard_02.distrito_nombre`
  - `pbi_dashboard_02.categoria_municipalidad`
  - `PIA Total`
  - `PIM Total`
  - `Recaudado Total`
  - `Pct Ejecucion`

Filtros:

- `pbi_dashboard_02.year`
- `pbi_dashboard_02.month`
- `pbi_dashboard_02.categoria_municipalidad`
- `pbi_dashboard_02.rubro_nombre`
- `pbi_dashboard_02.generica_nombre`

Titulo sugerido:

```text
Peru. Analisis de recaudacion municipal segun clasificador de ingreso.
```

## Pagina 3: Predial vs efectividad

Tabla:

```text
03_predial_vs_efectividad
```

Objetivo:

Evaluar el rendimiento del impuesto predial comparando recaudacion, emision y efectividad por territorio y categoria.

Visuales:

- Cards:
  - `Recaudacion Predial`
  - `Emision Predial`
  - `Efectividad Predial`
  - `Brecha Predial`
  - `Municipalidades Predial`
- Barras horizontales:
  - Eje: `pbi_dashboard_03.departamento_nombre`
  - Valor: `Recaudacion Predial`
  - Orden: descendente
- Scatter:
  - X: `pbi_dashboard_03.efectividad_predial_pct`
  - Y: `pbi_dashboard_03.recaudacion_predial_total`
  - Size: `pbi_dashboard_03.emision_predial_total`
  - Legend: `pbi_dashboard_03.categoria_municipalidad`
  - Details: `pbi_dashboard_03.sec_ejec`
- Tabla:
  - `pbi_dashboard_03.departamento_nombre`
  - `pbi_dashboard_03.provincia_nombre`
  - `pbi_dashboard_03.distrito_nombre`
  - `pbi_dashboard_03.estado_sismepre`
  - `pbi_dashboard_03.clasificacion_sismepre`
  - `pbi_dashboard_03.tipo_meta`
  - `Recaudacion Predial`
  - `Emision Predial`
  - `Efectividad Predial`

Filtros:

- `pbi_dashboard_03.year`
- `pbi_dashboard_03.month`
- `pbi_dashboard_03.categoria_municipalidad`
- `pbi_dashboard_03.tipo_meta`
- `pbi_dashboard_03.estado_sismepre`

Titulo sugerido:

```text
Peru. Recaudacion del impuesto predial municipal vs indicador de efectividad.
```

## Pagina 4: Distribucion de efectividad predial

Tabla:

```text
04_distribucion_efectividad
```

Objetivo:

Analizar la distribucion de la efectividad predial por departamento, categoria y deciles.

Visuales:

- Cards:
  - `Efectividad Predial Promedio`
  - `Recaudacion Predial Dist`
  - `Emision Predial Dist`
  - `Municipalidades Distribucion`
- Barras horizontales:
  - Eje: `pbi_dashboard_04.departamento_nombre`
  - Valor: `Efectividad Predial Promedio`
  - Orden: descendente
- Histograma o columnas:
  - Eje: `pbi_dashboard_04.decil_efectividad`
  - Valor: conteo de `pbi_dashboard_04.sec_ejec`
- Tabla ranking:
  - `pbi_dashboard_04.departamento_nombre`
  - `pbi_dashboard_04.provincia_nombre`
  - `pbi_dashboard_04.distrito_nombre`
  - `pbi_dashboard_04.categoria_municipalidad`
  - `pbi_dashboard_04.ranking_departamental`
  - `pbi_dashboard_04.efectividad_predial_pct`

Filtros:

- `pbi_dashboard_04.year`
- `pbi_dashboard_04.categoria_municipalidad`
- `pbi_dashboard_04.departamento_nombre`
- `pbi_dashboard_04.decil_efectividad`

Titulo sugerido:

```text
Peru. Distribucion territorial de la efectividad del impuesto predial.
```

## Pagina 5: Software tributario municipal

Tabla:

```text
05_software_tributario
```

Objetivo:

Evaluar la capacidad tecnologica tributaria municipal usando RENAMU: SRTM, software propio de rentas, catastro y al menos un software tributario.

Visuales:

- Cards:
  - `Municipalidades Software`
  - `Municipalidades Con Software AT`
  - `Municipalidades Con SRTM`
  - `Municipalidades Con Software Rentas`
  - `Municipalidades Con Software Catastro`
  - `Pct Con Software AT`
- Barras apiladas:
  - Eje: `pbi_dashboard_05.departamento_nombre`
  - Leyenda: `pbi_dashboard_05.usa_srtm_estado`
  - Valor: conteo de `pbi_dashboard_05.sec_ejec`
- Barras apiladas:
  - Eje: `pbi_dashboard_05.departamento_nombre`
  - Leyenda: `pbi_dashboard_05.usa_software_rentas_at`
  - Valor: conteo de `pbi_dashboard_05.sec_ejec`
- Barras apiladas:
  - Eje: `pbi_dashboard_05.departamento_nombre`
  - Leyenda: `pbi_dashboard_05.usa_software_catastro`
  - Valor: conteo de `pbi_dashboard_05.sec_ejec`
- Tabla:
  - `pbi_dashboard_05.departamento_nombre`
  - `pbi_dashboard_05.provincia_nombre`
  - `pbi_dashboard_05.distrito_nombre`
  - `pbi_dashboard_05.categoria_municipalidad`
  - `pbi_dashboard_05.usa_srtm_estado`
  - `pbi_dashboard_05.usa_software_rentas_at`
  - `pbi_dashboard_05.usa_software_catastro`
  - `pbi_dashboard_05.usa_al_menos_un_software_at`

Filtros:

- `pbi_dashboard_05.year`
- `pbi_dashboard_05.categoria_municipalidad`
- `pbi_dashboard_05.departamento_nombre`
- `pbi_dashboard_05.usa_al_menos_un_software_at`

Titulo sugerido:

```text
Peru. Implementacion de software de recaudacion en municipalidades.
```

## Pagina 6: Priorizacion de municipalidades

Tabla:

```text
06_priorizacion_municipal
```

Objetivo:

Priorizar municipalidades para asistencia, fiscalizacion o mejora de gestion tributaria cruzando recaudacion, brecha predial, efectividad, software y SISMEPRE.

Visuales:

- Cards:
  - `Recaudacion Total Priorizacion`
  - `Base Imponible Predial`
  - `Recaudacion Predial Priorizacion`
  - `Saldo Predial Total`
  - `Municipalidades Alta Prioridad`
  - `Pct Alta Prioridad`
- Medidor:
  - Valor: `Efectividad Priorizacion Promedio`
- Tabla principal:
  - `pbi_dashboard_06.ubigeo`
  - `pbi_dashboard_06.municipalidad_nombre`
  - `pbi_dashboard_06.departamento_nombre`
  - `pbi_dashboard_06.provincia_nombre`
  - `pbi_dashboard_06.distrito_nombre`
  - `pbi_dashboard_06.categoria_municipalidad`
  - `pbi_dashboard_06.recaudacion_total`
  - `pbi_dashboard_06.recaudacion_predial_total`
  - `pbi_dashboard_06.base_imponible_predial`
  - `pbi_dashboard_06.saldo_predial_total`
  - `pbi_dashboard_06.efectividad_predial_pct`
  - `pbi_dashboard_06.usa_al_menos_un_software_at`
  - `pbi_dashboard_06.estado_sismepre`
  - `pbi_dashboard_06.clasificacion_sismepre`
  - `pbi_dashboard_06.tipo_meta_sismepre`
  - `pbi_dashboard_06.prioridad_intervencion`
- Barras:
  - Eje: `pbi_dashboard_06.prioridad_intervencion`
  - Valor: conteo de `pbi_dashboard_06.sec_ejec`

Filtros:

- `pbi_dashboard_06.year`
- `pbi_dashboard_06.categoria_municipalidad`
- `pbi_dashboard_06.departamento_nombre`
- `pbi_dashboard_06.prioridad_intervencion`
- `pbi_dashboard_06.usa_al_menos_un_software_at`

Titulo sugerido:

```text
Peru. Priorizacion de municipalidades para focalizacion de estrategias tributarias.
```

## Medidas creadas en el modelo

Medidas principales disponibles:

- `Recaudacion Total`
- `Personal Municipal`
- `Recaudacion Por Personal`
- `PIM Total Capacidad`
- `Municipalidades Capacidad`
- `Pct Ejecucion Capacidad`
- `PIA Total`
- `PIM Total`
- `Recaudado Total`
- `Pct Ejecucion`
- `Variacion PIM PIA`
- `Municipalidades Clasificador`
- `Recaudacion Predial`
- `Emision Predial`
- `Efectividad Predial`
- `Brecha Predial`
- `Municipalidades Predial`
- `Efectividad Predial Promedio`
- `Recaudacion Predial Dist`
- `Emision Predial Dist`
- `Municipalidades Distribucion`
- `Municipalidades Con Software AT`
- `Municipalidades Con SRTM`
- `Municipalidades Con Software Rentas`
- `Municipalidades Con Software Catastro`
- `Municipalidades Software`
- `Municipalidades Sin Software AT`
- `Pct Con Software AT`
- `Saldo Predial Total`
- `Recaudacion Predial Priorizacion`
- `Efectividad Priorizacion Promedio`
- `Recaudacion Total Priorizacion`
- `Base Imponible Predial`
- `Municipalidades Priorizacion`
- `Municipalidades Alta Prioridad`
- `Pct Alta Prioridad`

## Comentario para sustentacion

Las tablas `pbi_dashboard_01` a `pbi_dashboard_06` son tablas analiticas finales construidas sobre el modelo Gold. El modelo formal sigue estando en dimensiones y facts; estas tablas existen para facilitar el consumo en Power BI y reducir joins pesados en la herramienta visual.

