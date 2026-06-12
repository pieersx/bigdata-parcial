# Reconstruccion Bronze, Silver y Gold - 2026-06-07

## Estado general

La reconstruccion de las capas medallion quedo ejecutada sobre las fuentes raw actualizadas.

- Bronze: ejecutado para `ingresos`, `sismepre` y `renamu`.
- Silver: ejecutado con estado `SUCCESS`.
- Gold: ejecutado con estado `SUCCESS`.
- Gold execution id: `20260607_003131`.
- Gold audit: `data/audit/executions/2026/06/07/20260607_005933_838687.json`.
- Gold metrics: `data/audit/metrics/2026/06/07/gold_summary_20260607_003131.json`.

## Correcciones aplicadas

- `app/services/landing_service.py`: las fuentes `mef_api` ahora comparan el total oficial remoto contra el total local del CSV raw. Si no coincide, el CSV raw se refresca automaticamente.
- `config.yaml`: RENAMU ahora se lee con `multiLine: true`, corrigiendo la perdida de filas al pasar de CSV a Parquet Bronze.

## Bronze validado

| Tabla | Filas |
|---|---:|
| `data/bronze/ingresos/year=2025` | 878,730 |
| `data/bronze/ingresos/year=2026` | 354,316 |
| `data/bronze/renamu` | 1,874 |
| `data/bronze/sismepre/rentas_preguntas` | 836 |
| `data/bronze/sismepre/rentas_formulario` | 98 |
| `data/bronze/sismepre/rentas_esat_estadistica_atm` | 133,810 |
| `data/bronze/sismepre/rentas_respuestas` | 174,210 |

## Silver validado

| Tabla | Filas |
|---|---:|
| `dim_municipalidad` | 1,112 |
| `fact_ingresos_municipales` | 8,950,779 |
| `fact_predial_esat` | 133,810 |
| `fact_sismepre_respuestas` | 205,823 |
| `dim_sismepre_entidad_estado` | 19,037 |
| `dim_sismepre_pregunta` | 836 |
| `dim_sismepre_formulario` | 98 |

## Gold validado

| Tabla | Filas |
|---|---:|
| `dim_municipalidad_gold` | 1,964 |
| `dim_tiempo` | 173 |
| `dim_clasificador_ingreso` | 2,791 |
| `fact_ingresos_mensuales` | 325,090 |
| `fact_ingresos_clasificador` | 8,950,758 |
| `fact_predial_mensual` | 49,602 |
| `fact_sismepre_cumplimiento` | 19,037 |
| `fact_sismepre_respuestas_resumen` | 205,533 |
| `fact_calidad_datos` | 4,167 |

## Resultado

Las tres capas quedan listas para continuar con Power BI. La diferencia principal frente al estado anterior es que SISMEPRE ya no esta desfasado y RENAMU Bronze conserva las 1,874 filas del CSV oficial extraido.

## Power BI

Workbook actualizado desde Gold:

- `data/powerbi/powerbi_municipal_gold.xlsx`
- Hojas principales de decision: `d1_evolucion`, `d2_avance_municipal`, `d3_ranking_territorial`, `d4_clasificador`, `d5_predial`, `d6_priorizacion`.
- Diseno documentado en `reports/powerbi_6_dashboards_municipales_20260607.md`.
