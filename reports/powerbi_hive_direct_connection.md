# Conexion Directa Power BI A Hive/Parquet

## Objetivo

Power BI debe consumir todos los registros Gold desde Hive, no desde Excel. Hive
registra los Parquet Gold como tablas externas sobre HDFS y expone consultas por
HiveServer2 en `localhost:10000`.

## Estado Actual Verificado

Cluster Docker activo:

- `namenode`
- `datanode`
- `hive-metastore-db`
- `hive-metastore`
- `hive-server`
- `transformers-networks`

Base Hive:

- Base de datos: `municipal_gold`
- Puerto HiveServer2: `10000`
- Driver recomendado: Cloudera Hive ODBC 64-bit

Conteos verificados por Beeline:

| Objeto Hive | Filas |
|---|---:|
| `fact_ingresos_clasificador` | `8,950,758` |
| `mart_dashboard_municipal` | `28,628` |
| `vw_dashboard_01_recaudacion_capacidad` | `28,628` |

## Tablas Recomendadas Para Los 6 Dashboards

Conectar Power BI a estas tablas materializadas. Son mas estables para ODBC
que las vistas porque tienen nombres cortos y tipos simples:

| Dashboard | Tabla Hive |
|---|---|
| Recaudacion Municipal Vs Capacidad Tributaria | `pbi_dashboard_01` |
| Recaudacion Por Clasificador De Ingreso | `pbi_dashboard_02` |
| Predial Vs Efectividad | `pbi_dashboard_03` |
| Distribucion De Efectividad Predial | `pbi_dashboard_04` |
| Software Tributario Municipal | `pbi_dashboard_05` |
| Priorizacion De Municipalidades | `pbi_dashboard_06` |

Las vistas `vw_dashboard_*` quedan como SQL analitico auditable, pero Power BI
debe importar preferentemente las tablas `pbi_dashboard_*`.

## Driver Descargado

Se descargo el instalador oficial:

```text
C:\Users\Pieers\Downloads\bigdata-drivers\ClouderaHiveODBC64.msi
```

Instalarlo manualmente antes de configurar el DSN ODBC.

## Configuracion Del DSN ODBC

Abrir:

```text
ODBC Data Sources (64-bit)
```

Crear un System DSN:

| Campo | Valor |
|---|---|
| DSN Name | `MunicipalHive` |
| Host | `localhost` |
| Port | `10000` |
| Database | `municipal_gold` |
| Hive Server Type | `Hive Server 2` |
| Transport | `Binary` |
| Authentication | `No Authentication` o `Anonymous` |
| User | `root` |
| Password | vacio |

## Conexion En Power BI Desktop

1. Abrir Power BI Desktop.
2. Ir a `Obtener datos`.
3. Elegir `ODBC`.
4. Seleccionar DSN `MunicipalHive`.
5. Importar las tablas `pbi_dashboard_01` a `pbi_dashboard_06`.
6. Crear cada pagina con su vista correspondiente.

## Validacion Desde Consola

Para verificar que Hive sigue disponible:

```powershell
docker compose exec -T hive-server beeline -u "jdbc:hive2://localhost:10000/municipal_gold" -n root -e "SHOW TABLES;"
```

Para verificar que se estan leyendo millones de registros:

```powershell
docker compose exec -T hive-server beeline -u "jdbc:hive2://localhost:10000/municipal_gold" -n root -e "SELECT COUNT(*) FROM fact_ingresos_clasificador;"
```

Para regenerar las tablas finales para Power BI:

```powershell
docker compose exec -T transformers-networks python scripts/materialize_powerbi_hive_tables.py
```

Si Power BI todavia muestra vistas `vw_dashboard_*`, cerrar el navegador de
Power BI y refrescar la conexion. El catalogo activo debe quedar solo con
`pbi_dashboard_*` para dashboards:

```powershell
docker compose exec -T hive-server beeline -u "jdbc:hive2://localhost:10000/municipal_gold" -n root -e "DROP VIEW IF EXISTS vw_dashboard_01_recaudacion_capacidad; DROP VIEW IF EXISTS vw_dashboard_02_clasificador_ingreso; DROP VIEW IF EXISTS vw_dashboard_03_predial_vs_efectividad; DROP VIEW IF EXISTS vw_dashboard_04_distribucion_efectividad; DROP VIEW IF EXISTS vw_dashboard_05_software_tributario; DROP VIEW IF EXISTS vw_dashboard_06_priorizacion_municipal;"
```

## Nota

El archivo Excel queda solo como respaldo de demostracion. La ruta defendible
para la exposicion es Power BI conectado a Hive/Parquet.
