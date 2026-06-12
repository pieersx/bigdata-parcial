# Cierre De Cumplimiento - Parcial BigData Municipal

## Estado Por Requisito

| Requisito | Estado | Implementacion |
|---|---|---|
| Integrar 3 fuentes | Cumple | SIAF Ingresos, SISMEPRE y RENAMU configurados en `config.yaml` y publicados en Bronze/Silver/Gold. |
| Solo municipalidades | Cumple | Silver filtra `NIVEL_GOBIERNO = 'M'`; Gold publica universo municipal SIAF. |
| Municipalidades presentadas | Preparado | `data/reference/municipalidades_presentadas.csv`; Gold usa `in_scope_presentacion` y audita el alcance. |
| Fuentes sin temporalidad | Cumple | RENAMU 2022 se trata como enriquecimiento estatico; no se convierte en serie temporal. |
| Modelo optimo | Cumple | Gold usa modelo estrella/constelacion con dimensiones y facts analiticos. |
| Normalizar SIAF | Cumple | SIAF se separa en `dim_tiempo`, `dim_municipalidad_gold`, `dim_clasificador_ingreso` y facts de ingresos. |
| Arquitectura Medallion | Cumple | Raw, Bronze, Silver, Gold, quarantine, audit y quality checks. |
| Pipeline de ingesta | Cumple | `main.py`, `main_silver.py`, `main_gold.py` y `scripts/live_siaf_2025_2026_demo.py`. |
| 6 dashboards decision | Cumple como modelo/export | Workbook `data/powerbi/powerbi_municipal_gold.xlsx` con 6 hojas de decision; el PBIX se refresca manualmente en Power BI Desktop. |
| Calidad documentada | Cumple | `data/audit/quality_checks`, `data/audit/metrics`, notebooks y `fact_calidad_datos`. |
| SIAF 2012-2024 | Cumple | CSV historicos oficiales configurados y cargados. |
| SIAF 2025-2026 vivo | Cumple para demo | CSV mensual oficial con validacion `Content-Length` y `Last-Modified`; demo con `scripts/live_siaf_2025_2026_demo.py`. |
| Categorias municipales | Cumple | `CategoriasMunicipalidades.csv` se publica en Bronze/Silver y enriquece `dim_municipalidad_gold` con `categoria_municipalidad` y `categoria_match_status`. |
| Mapas | No aplica | Las tres fuentes no traen latitud, longitud ni geometria; se usan rankings y segmentadores territoriales. |

## Comando De Exposicion

```powershell
docker compose run --rm --no-deps transformers-networks python scripts/live_siaf_2025_2026_demo.py
```

El comando muestra metadata remota/local de SIAF 2025-2026, reconstruye solo esas particiones Bronze de ingresos, luego Silver y Gold, y regenera el workbook de Power BI.

## Nota De Alcance

Si el profesor entrega una lista de municipalidades, reemplazar la plantilla `data/reference/municipalidades_presentadas.csv` con las filas del archivo. Si la plantilla queda vacia, Gold conserva todas las municipalidades y registra `pendiente_archivo_profesor`.

El archivo de categorias no trae `SEC_EJEC` ni `UBIGEO`; por eso el match se hace por nombre normalizado y no se asigna categoria cuando el nombre es ambiguo.
