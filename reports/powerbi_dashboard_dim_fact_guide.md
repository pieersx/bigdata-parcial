# Guia Power BI: 6 Dashboards Municipales con Modelo Gold

Archivo base: `data/powerbi/Dashboard_Municipal_Gold_Oficial.pbix`

Esta guia usa el modelo dimensional Gold cargado desde Parquet. La idea es trabajar con dimensiones y facts, no solo con tablas planas `pbi_dashboard_01..06`. Las tablas `pbi_dashboard_*` pueden servir como apoyo rapido, pero el modelo correcto para explicar al profesor es el modelo estrella/constelacion.

## Estado Del Modelo

El modelo Power BI queda con:

- Tabla de medidas: `_Medidas`.
- Dimensiones principales:
  - `dim_municipalidad_gold`
  - `dim_tiempo`
  - `dim_clasificador_ingreso`
  - `dim_ubigeo`
  - `dim_estado_sismepre`
  - `dim_formulario_sismepre`
  - `dim_pregunta_sismepre`
- Facts principales:
  - `fact_ingresos_mensuales`
  - `fact_ingresos_clasificador`
  - `fact_predial_mensual`
  - `fact_sismepre_cumplimiento`
  - `fact_sismepre_respuestas_resumen`
  - `fact_renamu_gestion_tributaria`
  - `fact_renamu_software_at`
  - `fact_calidad_datos`
- Marts:
  - `mart_dashboard_municipal`
  - `mart_kpi_resumen_ejecutivo`

Validacion aplicada en el PBIX:

- `dim_municipalidad_gold`: 1,964 municipalidades.
- Municipalidades con categoria: 1,713.
- `fact_ingresos_mensuales`: 292,340 filas.
- La tabla `dim_municipalidad_gold` ya no debe tener filtro solo a Santa Rosa.

## Medidas DAX Creadas

Las medidas estan en `_Medidas` y organizadas por carpetas:

- `00 Cobertura`
  - `Total Municipalidades`
  - `Municipalidades con Categoria`
  - `% Cobertura Categoria`
- `01 Ingresos`
  - `Total PIA`
  - `Total PIM`
  - `Total Recaudado`
  - `Variacion PIM vs PIA`
  - `% Ejecucion`
  - `Brecha Recaudacion`
- `01 Ingresos Clasificador`
  - `Total PIA Clasificador`
  - `Total PIM Clasificador`
  - `Total Recaudado Clasificador`
  - `% Ejecucion Clasificador`
- `02 Predial`
  - `Recaudacion Predial Total`
  - `Emision Predial Afecta`
  - `Saldo Predial Total`
  - `% Efectividad Predial`
  - `Contribuyentes Predio`
  - `Recaudacion Predial Ordinaria Actual`
  - `Recaudacion Predial Coactiva Actual`
  - `Saldo Predial Ordinario`
  - `Saldo Predial Coactivo`
- `03 RENAMU`
  - `Personal Municipal Total`
  - `Recaudacion por Personal`
  - `Municipalidades con SRTM`
  - `Municipalidades con Software Rentas`
  - `Municipalidades con Catastro`
  - `Municipalidades con algun Software AT`
  - `Municipalidades Requieren Asistencia AT`
  - `Municipalidades Requieren Capacitacion AT`
- `04 SISMEPRE`
  - `Municipalidades SISMEPRE`
  - `Registros SISMEPRE`
  - `Municipalidades Cumplimiento SISMEPRE`
  - `% Cobertura SISMEPRE`
- `05 Calidad`
  - `Checks Calidad`
  - `Checks Fallidos`
  - `% Fallo Calidad`
- `06 Priorizacion`
  - `Municipalidades Priorizadas`

## Columnas Que No Debes Mostrar

Estas columnas sirven para trazabilidad, auditoria o tecnica interna. No las uses en visuales principales:

- Cualquier columna que empiece con `_bronze_`, `_silver_` o `_gold_`.
- Columnas raw de RENAMU: `P16_5_raw`, `P17_2_raw`, `P17_3_raw`, `P19M_T_raw`, `P22_AT2_raw`, `P22_AT3_raw`, `P22_C2_raw`, `P22_C3_raw`.
- Campos tecnicos RENAMU en la dimension municipal: `idmunici`, `Tipomuni`, `RENAMU_DEPARTAMENTO`, `RENAMU_PROVINCIA`, `RENAMU_DISTRITO`.
- IDs tecnicos salvo que sean necesarios para relaciones: `periodo_id`, `clasificador_id`, `estado_sismepre_id`.

Para los dashboards usa nombres de negocio:

- Municipalidad, departamento, provincia, distrito.
- Categoria municipal A-G.
- Ano, mes, trimestre.
- PIA, PIM, recaudado, ejecucion, brecha.
- Personal AT, SRTM, software rentas, catastro.
- Recaudacion predial, emision, saldo, efectividad.

## Diseno Visual Base

Estilo recomendado, inspirado en el ejemplo del MEF:

- Fondo de pagina: verde claro `#DDF8D4`.
- Encabezado superior: verde fuerte `#10A72C`.
- Texto del encabezado: blanco.
- Paneles de visuales: blanco o verde muy claro.
- Barras principales: verde `#45B80F`.
- Alertas o brechas: rojo suave `#E85C5C`.
- Scatter predial: naranja `#FF8A1C`.
- Tablas: encabezado verde, texto blanco.

Estructura de cada pagina:

1. Titulo grande arriba.
2. Segmentador horizontal de anos.
3. Segmentador lateral de categoria A-G.
4. Tarjetas KPI en la parte superior.
5. Graficos principales al centro.
6. Tabla o ranking a la derecha o abajo.
7. Pie: `Fuente: SIAF, SISMEPRE, RENAMU y CategoriasMunicipalidades.csv`.

## Dashboard 1: Recaudacion Municipal Vs Capacidad Tributaria

Objetivo: comparar la recaudacion municipal SIAF con la capacidad administrativa RENAMU.

Tablas:

- `fact_ingresos_mensuales`
- `fact_renamu_gestion_tributaria`
- `dim_municipalidad_gold`
- `dim_tiempo`

Filtros:

- `dim_tiempo[year]`
- `dim_municipalidad_gold[categoria_municipalidad]`
- `dim_municipalidad_gold[DEPARTAMENTO_NOMBRE]`

KPIs:

- `Total Recaudado`
- `Personal Municipal Total`
- `Recaudacion por Personal`
- `% Ejecucion`

Visuales:

- Barras horizontales: departamento vs `Personal Municipal Total`.
- Barras horizontales: departamento vs `Total Recaudado`.
- Scatter:
  - Eje X: `Personal Municipal Total`
  - Eje Y: `Total Recaudado`
  - Leyenda: `categoria_municipalidad`
  - Detalles: `MUNICIPALIDAD_NOMBRE`
- Tabla:
  - Municipalidad
  - Departamento
  - Categoria
  - Total Recaudado
  - Personal Municipal Total
  - Recaudacion por Personal

Lectura para exposicion:

> Este tablero permite identificar municipalidades que recaudan mucho con poco personal o municipalidades con personal disponible pero baja recaudacion.

## Dashboard 2: Recaudacion Por Clasificador De Ingreso

Objetivo: explicar de donde provienen los ingresos municipales.

Tablas:

- `fact_ingresos_clasificador`
- `dim_clasificador_ingreso`
- `dim_municipalidad_gold`
- `dim_tiempo`

Filtros:

- Ano
- Categoria A-G
- Departamento
- Rubro
- Especifica de ingreso

KPIs:

- `Total PIA Clasificador`
- `Total PIM Clasificador`
- `Total Recaudado Clasificador`
- `% Ejecucion Clasificador`

Visuales:

- Ranking por `dim_clasificador_ingreso[ESPECIFICA_DET]` usando `Total Recaudado Clasificador`.
- Ranking por `dim_clasificador_ingreso[RUBRO]`.
- Tabla territorial:
  - Departamento
  - Provincia
  - Distrito
  - Municipalidad
  - Rubro
  - Especifica
  - Total Recaudado Clasificador
- Matriz:
  - Filas: Rubro
  - Columnas: Ano
  - Valores: Total Recaudado Clasificador

Lectura para exposicion:

> Este tablero muestra si la municipalidad depende del predial, limpieza publica, alcabala u otros recursos. Ayuda a decidir donde fortalecer la gestion de ingresos.

## Dashboard 3: Predial Vs Efectividad

Objetivo: analizar la relacion entre recaudacion predial y efectividad de cobranza.

Tablas:

- `fact_predial_mensual`
- `dim_municipalidad_gold`
- `dim_tiempo`
- `fact_sismepre_cumplimiento`

Filtros:

- Ano
- Categoria A-G
- Departamento
- Tipo de meta

KPIs:

- `Recaudacion Predial Total`
- `Emision Predial Afecta`
- `Saldo Predial Total`
- `% Efectividad Predial`

Visuales:

- Scatter:
  - Eje X: `% Efectividad Predial`
  - Eje Y: `Recaudacion Predial Total`
  - Leyenda: Categoria municipal
  - Detalles: Municipalidad
- Barras: departamento vs `Recaudacion Predial Total`.
- Tabla:
  - Municipalidad
  - Categoria
  - Emision Predial Afecta
  - Recaudacion Predial Total
  - Saldo Predial Total
  - `% Efectividad Predial`

Lectura para exposicion:

> Este tablero ayuda a encontrar municipalidades con alto potencial predial pero baja efectividad, que son candidatas para apoyo o fiscalizacion.

## Dashboard 4: Distribucion De Efectividad Predial

Objetivo: entender como se distribuye la efectividad predial entre territorios y categorias.

Tablas:

- `fact_predial_mensual`
- `dim_municipalidad_gold`
- `dim_tiempo`

Filtros:

- Ano
- Categoria A-G
- Departamento

KPIs:

- `% Efectividad Predial`
- `Recaudacion Predial Total`
- `Saldo Predial Total`
- `Contribuyentes Predio`

Visuales:

- Barras horizontales: departamento vs `% Efectividad Predial`.
- Barras por categoria: categoria A-G vs `% Efectividad Predial`.
- Histograma:
  - Usar bins de `% Efectividad Predial`.
  - Conteo de municipalidades.
- Tabla:
  - Municipalidad
  - Departamento
  - Categoria
  - Efectividad
  - Recaudacion Predial

Lectura para exposicion:

> Este tablero permite ver si las categorias municipales tienen comportamientos distintos y donde se concentran los niveles bajos de efectividad.

## Dashboard 5: Software Tributario Municipal

Objetivo: evaluar la modernizacion tecnologica de la gestion tributaria municipal.

Tablas:

- `fact_renamu_software_at`
- `fact_renamu_gestion_tributaria`
- `dim_municipalidad_gold`

Filtros:

- Categoria A-G
- Departamento
- Provincia

KPIs:

- `Municipalidades con SRTM`
- `Municipalidades con Software Rentas`
- `Municipalidades con Catastro`
- `Municipalidades con algun Software AT`

Visuales:

- Barras apiladas: departamento vs `usa_srtm_estado`.
- Barras apiladas: departamento vs `usa_software_rentas_at`.
- Barras apiladas: departamento vs `usa_software_catastro`.
- Tabla:
  - Municipalidad
  - Categoria
  - Usa SRTM
  - Usa software de rentas
  - Usa software de catastro
  - Usa al menos un software AT

Lectura para exposicion:

> Este tablero permite detectar brechas tecnologicas. Una municipalidad sin software tributario puede tener dificultades para sostener una recaudacion eficiente.

## Dashboard 6: Priorizacion De Municipalidades

Objetivo: construir una lista accionable de municipalidades a priorizar.

Tablas:

- `dim_municipalidad_gold`
- `fact_ingresos_mensuales`
- `fact_predial_mensual`
- `fact_renamu_software_at`
- `fact_sismepre_cumplimiento`

Filtros:

- Ano
- Categoria A-G
- Departamento
- Provincia

KPIs:

- `Brecha Recaudacion`
- `Saldo Predial Total`
- `% Efectividad Predial`
- `Municipalidades Priorizadas`

Visuales:

- Tabla principal ordenada por mayor brecha:
  - Departamento
  - Provincia
  - Distrito
  - Municipalidad
  - Categoria
  - Total PIM
  - Total Recaudado
  - Brecha Recaudacion
  - Recaudacion Predial Total
  - Saldo Predial Total
  - Efectividad Predial
  - Usa software AT
  - Estado SISMEPRE
- Medidor:
  - Valor: `% Efectividad Predial`
- Tarjetas:
  - Brecha Recaudacion
  - Saldo Predial Total
  - Municipalidades Priorizadas

Regla de lectura:

> Priorizar primero municipalidades con alta brecha, alto saldo predial, baja efectividad y ausencia de software tributario.

## Relaciones Recomendadas

Mantener activas estas relaciones principales:

- `dim_municipalidad_gold[SEC_EJEC]` hacia facts SIAF, predial y SISMEPRE.
- `dim_municipalidad_gold[UBIGEO]` hacia facts RENAMU cuando aplique.
- `dim_tiempo[periodo_id]` hacia facts mensuales.
- `dim_clasificador_ingreso[clasificador_id]` hacia `fact_ingresos_clasificador`.
- `dim_estado_sismepre[estado_sismepre_id]` hacia `fact_sismepre_cumplimiento`.

Direccion recomendada:

- De dimensiones hacia facts.
- Evitar relaciones many-to-many salvo que sean estrictamente necesarias.

## Como Usar Categoria A-G

La categoria municipal viene de `CategoriasMunicipalidades.csv` y se aplica en Silver/Gold como atributo de `dim_municipalidad_gold`.

En Power BI:

1. Inserta un segmentador.
2. Campo: `dim_municipalidad_gold[categoria_municipalidad]`.
3. Orientacion: vertical.
4. Valores: A, B, C, D, E, F, G.
5. Ubicacion: lado izquierdo, igual al ejemplo.

Interpretacion:

- A-G no es una metrica calculada por Power BI.
- Es una clasificacion entregada como archivo maestro.
- Sirve para comparar grupos de municipalidades.

## Recomendacion Final

Para la exposicion, abre primero la vista Modelo y explica:

1. Las dimensiones describen territorio, tiempo, clasificador y catalogos.
2. Los facts contienen eventos o metricas medibles.
3. Las medidas DAX calculan KPIs.
4. Los dashboards consumen medidas, no columnas sueltas.

Luego pasa a las paginas de dashboard y cuenta la historia:

1. Capacidad tributaria.
2. Origen de ingresos.
3. Predial y efectividad.
4. Distribucion territorial.
5. Software tributario.
6. Priorizacion municipal.
