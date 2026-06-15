# Power BI Directo Desde Parquet Gold

## Objetivo

Power BI debe consumir la capa Gold directamente desde archivos Parquet. No se
usa Hive, ODBC ni Excel como fuente principal. El pipeline Gold publica las
tablas finales bajo `data/gold`, incluyendo seis tablas listas para dashboards:

- `pbi_dashboard_01`
- `pbi_dashboard_02`
- `pbi_dashboard_03`
- `pbi_dashboard_04`
- `pbi_dashboard_05`
- `pbi_dashboard_06`

## Ejecutar Pipelines

Desde la raiz del proyecto:

```powershell
docker compose build transformers-networks
docker compose up -d transformers-networks
docker compose exec transformers-networks python main.py
docker compose exec transformers-networks python main_silver.py
docker compose exec transformers-networks python main_gold.py
```

Gold lee exclusivamente Parquet de `data/silver` y escribe Parquet en
`data/gold`.

## Cargar En Power BI Desktop

Para cada tabla final:

1. Abrir Power BI Desktop.
2. Seleccionar **Obtener datos**.
3. Elegir **Carpeta**.
4. Seleccionar una carpeta, por ejemplo:

```text
C:\Users\Pieers\dev\bigdata\parcial\BigData\data\gold\pbi_dashboard_01
```

5. Elegir **Transformar datos**.
6. En Power Query, filtrar `Extension = .parquet`.
7. Usar **Combinar archivos**.
8. Renombrar la consulta como `pbi_dashboard_01`.
9. Repetir para `pbi_dashboard_02` a `pbi_dashboard_06`.

Si Power BI muestra carpetas `year=2025`, entrar desde **Carpeta** en la raiz de
la tabla y activar combinacion de archivos. Power Query debe leer los Parquet de
forma recursiva.

## Consulta M Base

Reemplazar el nombre de carpeta segun la tabla:

```powerquery
let
    Source = Folder.Files("C:\Users\Pieers\dev\bigdata\parcial\BigData\data\gold\pbi_dashboard_01"),
    ParquetFiles = Table.SelectRows(Source, each [Extension] = ".parquet"),
    WithData = Table.AddColumn(ParquetFiles, "Data", each Parquet.Document([Content])),
    Expanded = Table.ExpandTableColumn(WithData, "Data", Table.ColumnNames(WithData{0}[Data])),
    RemovedFiles = Table.RemoveColumns(Expanded, {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"})
in
    RemovedFiles
```

## Paginas Del Dashboard

| Pagina | Tabla Parquet | Objetivo |
|---|---|---|
| 1. Recaudacion Municipal vs Capacidad Tributaria | `pbi_dashboard_01` | Comparar recaudacion SIAF con personal y capacidad tributaria RENAMU. |
| 2. Recaudacion por Clasificador | `pbi_dashboard_02` | Analizar origen de ingresos por rubro, generica y especifica. |
| 3. Predial vs Efectividad | `pbi_dashboard_03` | Relacionar recaudacion predial, saldo y recuperacion. |
| 4. Distribucion de Efectividad Predial | `pbi_dashboard_04` | Ver distribucion territorial y por categoria A-G. |
| 5. Software Tributario Municipal | `pbi_dashboard_05` | Comparar SRTM, software de rentas y catastro. |
| 6. Priorizacion Municipal | `pbi_dashboard_06` | Priorizar municipalidades por brecha predial, software y cumplimiento. |

## Medidas DAX Recomendadas

```DAX
Recaudacion = SUM(pbi_dashboard_01[MONTO_RECAUDADO])
PIM = SUM(pbi_dashboard_01[MONTO_PIM])
PIA = SUM(pbi_dashboard_01[MONTO_PIA])
Pct Ejecucion = DIVIDE([Recaudacion], [PIM])
Personal Municipal = SUM(pbi_dashboard_01[personal_municipal_total])
Recaudacion Por Personal = DIVIDE([Recaudacion], [Personal Municipal])

Recaudacion Clasificador = SUM(pbi_dashboard_02[MONTO_RECAUDADO])
Participacion Clasificador =
DIVIDE(
    [Recaudacion Clasificador],
    CALCULATE([Recaudacion Clasificador], ALL(pbi_dashboard_02[ESPECIFICA_DET_NOMBRE]))
)

Recaudacion Predial = SUM(pbi_dashboard_03[MON_RECAUDACION_TOTAL])
Saldo Predial = SUM(pbi_dashboard_03[MON_SALDO_PREDIAL_TOTAL])
Pct Recuperacion Predial = DIVIDE([Recaudacion Predial], [Recaudacion Predial] + [Saldo Predial])

Municipalidades = DISTINCTCOUNT(pbi_dashboard_06[SEC_EJEC])
Con SRTM = CALCULATE(DISTINCTCOUNT(pbi_dashboard_05[SEC_EJEC]), pbi_dashboard_05[usa_srtm_estado] = TRUE())
Con Software Rentas = CALCULATE(DISTINCTCOUNT(pbi_dashboard_05[SEC_EJEC]), pbi_dashboard_05[usa_software_rentas_at] = TRUE())
Con Software Catastro = CALCULATE(DISTINCTCOUNT(pbi_dashboard_05[SEC_EJEC]), pbi_dashboard_05[usa_software_catastro] = TRUE())
Brecha Predial = SUM(pbi_dashboard_06[kpi_brecha_predial])
```

## Segmentadores

Usar en todas las paginas:

- `year` o `ANO_DOC` / `ANO_ESTADISTICA`
- `categoria_municipalidad`
- `DEPARTAMENTO_NOMBRE`
- `PROVINCIA_NOMBRE`
- `DISTRITO_NOMBRE`

No se usan mapas externos porque las fuentes obligatorias traen ubicacion
administrativa, pero no latitud, longitud ni geometria.
