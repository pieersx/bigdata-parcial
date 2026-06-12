# Propuesta De 6 Dashboards Power BI - Ingresos Municipales

Fuente analitica: `data/gold` y workbook `data/powerbi/powerbi_municipal_gold.xlsx`.

Estilo recomendado, inspirado en el ejemplo Power BI del enunciado:

- Franja superior verde con titulo Peru / municipalidades.
- Botones o slicer de anio en la parte superior.
- KPIs grandes al centro: recaudacion, PIM, ejecucion, brecha.
- Filtro lateral por departamento, provincia, categoria, clasificacion o tipo de meta.
- Graficos principales en dos columnas.
- Pie de pagina con fuente: SIAF, SISMEPRE, RENAMU / elaboracion propia.

## 1. Evolucion Mensual Del Presupuesto Y Recaudacion

Pregunta de decision: como evoluciona la recaudacion municipal frente al PIA y PIM?

Hoja Power BI: `d1_evolucion`

KPIs:

- Recaudacion total.
- PIM total.
- PIA total.
- Porcentaje de ejecucion.
- Variacion PIM - PIA.

Visuales:

- Linea de tiempo mensual con `MONTO_PIA`, `MONTO_PIM` y `MONTO_RECAUDADO`.
- Barras por anio de recaudacion.
- Tabla de meses con mayor recaudacion.
- Slicers: anio y mes.

## 2. Avance De Ejecucion Por Municipalidad

Pregunta de decision: que municipalidades ejecutan mejor o peor su presupuesto de ingresos?

Hoja Power BI: `d2_avance_municipal`

KPIs:

- Recaudacion municipal.
- PIM municipal.
- Porcentaje de ejecucion.
- Numero de municipalidades analizadas.

Visuales:

- Ranking horizontal de municipalidades por recaudacion.
- Scatter: PIM vs recaudacion, con tamano por PIM y color por departamento.
- Tabla detalle: departamento, provincia, distrito, municipalidad, PIA, PIM, recaudado y ejecucion.
- Slicers: anio, departamento, provincia, municipalidad y categoria.

## 3. Ranking Territorial Por Recaudacion

Pregunta de decision: que departamentos y provincias concentran la mayor recaudacion municipal?

Hoja Power BI: `d3_ranking_territorial`

KPIs:

- Recaudacion territorial.
- Municipalidades por territorio.
- PIM territorial.
- Porcentaje de ejecucion territorial.

Visuales:

- Barras por departamento ordenadas por recaudacion.
- Tabla provincia / departamento con PIM, recaudacion y ejecucion.
- Barras y tabla territorial por departamento, provincia y distrito. No se usa mapa externo porque las tres fuentes no traen latitud, longitud ni geometria.
- Slicers: anio, departamento, provincia y categoria.

## 4. Recaudacion Segun Clasificador De Ingreso

Pregunta de decision: de que rubros y clasificadores proviene la recaudacion municipal?

Hoja Power BI: `d4_clasificador`

KPIs:

- Recaudacion por clasificador.
- Participacion del rubro en el total.
- PIA, PIM y recaudado por rubro.
- Variacion PIM - PIA.

Visuales:

- Barras horizontales por `RUBRO_NOMBRE`.
- Matriz drill-down: fuente de financiamiento, rubro, generica, subgenerica, especifica.
- Tabla top clasificadores con monto recaudado.
- Slicers: anio, fuente, rubro, generica y especifica.

## 5. Indicadores De Impuesto Predial

Pregunta de decision: como se comporta la recaudacion predial y cual es la brecha por cobrar?

Hoja Power BI: `d5_predial`

KPIs:

- Recaudacion predial total.
- Recaudacion ordinaria.
- Recaudacion coactiva.
- Saldo predial.
- Base imponible y contribuyentes.

Visuales:

- Barras por departamento de recaudacion predial.
- Linea mensual de recaudacion predial.
- Scatter: saldo predial vs recaudacion predial.
- Tabla municipal de predial: municipalidad, base imponible, predios, contribuyentes, saldo y recaudacion.
- Slicers: anio estadistico, mes, departamento, categoria y tipo de meta.

## 6. Priorizacion Municipal Por Brecha Y Cumplimiento

Pregunta de decision: que municipalidades deben priorizarse por brecha predial y estado SISMEPRE?

Hojas Power BI: `d6_priorizacion` y `d6_sismepre`

KPIs:

- Saldo predial total.
- Base imponible.
- Recaudacion predial.
- Porcentaje de recuperacion predial.
- Municipalidades por estado SISMEPRE.

Visuales:

- Tabla de priorizacion municipal ordenada por saldo predial y baja recuperacion.
- Barras por estado SISMEPRE y clasificacion.
- Segmentadores de rango: recuperacion, saldo, departamento, categoria, clasificacion y estado.
- Tarjetas de conteo: municipalidades con informacion SISMEPRE y municipalidades solo SIAF.

## Hojas Auxiliares Del Workbook

Estas hojas no son dashboards de decision, pero ayudan a construir y defender el reporte:

| Hoja | Uso |
|---|---|
| `municipios` | Dimension geografica y flags de cobertura SIAF/SISMEPRE/RENAMU. |
| `d6_sismepre` | Filtros y detalle de cumplimiento SISMEPRE para la pagina 6. |
| `dashboard_diseno` | Resumen de paginas, preguntas, KPIs, visuales y filtros. |
| `medidas_dax` | Medidas sugeridas para Power BI. |
| `evidencia_calidad` | Evidencia tecnica del control de calidad; no usar como dashboard principal. |

## Estado Frente Al Enunciado

| Requisito | Estado | Evidencia |
|---|---|---|
| Arquitectura medallion | Completo | Raw, Bronze, Silver y Gold bajo `data/`. |
| Pipelines de ingesta | Completo | `main.py`, `main_silver.py`, `main_gold.py`. |
| 6 dashboards de decision | Preparado | Workbook actualizado y especificacion de paginas. |
| Calidad documentada | Completo | `data/audit`, notebooks y `fact_calidad_datos`. |
| Municipalidades presentadas | Preparado | `data/reference/municipalidades_presentadas.csv`; si esta vacio, Gold marca `pendiente_archivo_profesor` y conserva todo el universo municipal. |

## Alcance Y Temporalidad

- SIAF Ingresos se trata como serie temporal mensual 2012-2026.
- SISMEPRE se trata con los anios y periodos disponibles en sus tablas oficiales.
- RENAMU 2022 se usa como fuente estatica de enriquecimiento municipal; no se interpreta como serie temporal.
- El campo `in_scope_presentacion` permite limitar los dashboards a las municipalidades que comparta el profesor. Mientras la plantilla este vacia, todas las municipalidades quedan en alcance.
- `categoria_municipalidad` permite segmentar por categorias `A-G`. `categoria_match_status` documenta si el match fue unico, ambiguo, no encontrado o si falta la fuente.
- No se incorporan mapas externos: las tres fuentes solo contienen ubicacion administrativa (`UBIGEO`, departamento, provincia y distrito), no coordenadas ni geometria.

## Pendiente Practico

El archivo `data/powerbi/Dashboard_Municipal_Gold.pbix` existe, pero no se modifico por CLI porque no hay herramienta Power BI automatizable disponible en PATH. Para cerrar visualmente en Power BI Desktop:

1. Abrir `data/powerbi/Dashboard_Municipal_Gold.pbix`.
2. Refrescar origen desde `data/powerbi/powerbi_municipal_gold.xlsx`.
3. Verificar o crear las seis paginas usando esta especificacion.
4. Exportar capturas o PDF para anexar al informe.
