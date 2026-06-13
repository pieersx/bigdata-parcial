# Notebooks Del Proyecto

Los notebooks estan separados por capa para que la evidencia sea facil de
defender:

| Carpeta | Contenido |
|---|---|
| `bronze/` | Un notebook oficial: ingesta, inventario, profiling y calidad Bronze de SIAF, SISMEPRE, RENAMU y categorias. |
| `silver/` | Limpieza, estandarizacion, calidad, cuarentena, reglas de categorias y profiling Silver profesional. |
| `gold/` | Modelo dimensional/constelacion, KPIs y preparacion para dashboards. |
| `hive/` | Laboratorio Hive, HDFS, Metastore, SQL analitico y Power BI via ODBC. |

El notebook Bronze oficial es `bronze/01_Bronze_Data_Profiling_Calidad.ipynb`.
Los notebooks Bronze separados por fuente fueron retirados para no duplicar
evidencia durante la exposicion.

Power BI debe consumir las tablas Hive `pbi_dashboard_01` a
`pbi_dashboard_06`; las vistas `vw_dashboard_*` solo se usan internamente para
materializar esas tablas y luego se eliminan del catalogo activo.
