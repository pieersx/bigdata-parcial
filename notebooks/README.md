# Notebooks Del Proyecto

Los notebooks estan separados por capa para que la evidencia sea facil de
defender:

| Carpeta | Contenido |
|---|---|
| `bronze/` | Ingesta Bronze y profiling de fuentes crudas aterrizadas como Parquet. |
| `silver/` | Limpieza, estandarizacion, calidad, cuarentena y profiling Silver. |
| `gold/` | Modelo dimensional/constelacion, KPIs y preparacion para dashboards. |
| `hive/` | Laboratorio Hive, HDFS, Metastore, SQL analitico y Power BI via ODBC. |

Power BI debe consumir las tablas Hive `pbi_dashboard_01` a
`pbi_dashboard_06`; las vistas `vw_dashboard_*` solo se usan internamente para
materializar esas tablas y luego se eliminan del catalogo activo.
