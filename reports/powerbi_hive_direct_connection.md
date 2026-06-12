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

## Vistas Para Los 6 Dashboards

Conectar Power BI a estas vistas:

| Dashboard | Vista Hive |
|---|---|
| Recaudacion Municipal Vs Capacidad Tributaria | `vw_dashboard_01_recaudacion_capacidad` |
| Recaudacion Por Clasificador De Ingreso | `vw_dashboard_02_clasificador_ingreso` |
| Predial Vs Efectividad | `vw_dashboard_03_predial_vs_efectividad` |
| Distribucion De Efectividad Predial | `vw_dashboard_04_distribucion_efectividad` |
| Software Tributario Municipal | `vw_dashboard_05_software_tributario` |
| Priorizacion De Municipalidades | `vw_dashboard_06_priorizacion_municipal` |

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
5. Importar las vistas `vw_dashboard_01_*` a `vw_dashboard_06_*`.
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

## Nota

El archivo Excel queda solo como respaldo de demostracion. La ruta defendible
para la exposicion es Power BI conectado a Hive/Parquet.
