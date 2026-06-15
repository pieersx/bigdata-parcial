# Notebooks Del Proyecto

Los notebooks estan separados por capa para que la evidencia sea facil de
defender:

| Carpeta | Contenido |
|---|---|
| `bronze/` | Un notebook oficial: ingesta, inventario, profiling y calidad Bronze de SIAF, SISMEPRE, RENAMU y categorias. |
| `silver/` | Limpieza, estandarizacion, calidad, cuarentena, reglas de categorias y profiling Silver profesional. |
| `gold/` | Modelo dimensional/constelacion, KPIs y preparacion de tablas Parquet para dashboards Power BI. |

El notebook Bronze oficial es `bronze/01_Bronze_Data_Profiling_Calidad.ipynb`.
Los notebooks Bronze separados por fuente fueron retirados para no duplicar
evidencia durante la exposicion.

Power BI debe consumir directamente las carpetas Parquet Gold
`data/gold/pbi_dashboard_01` a `data/gold/pbi_dashboard_06`. No se usa Hive ni
ODBC en el flujo final.
