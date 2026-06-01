import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS_PATH = PROJECT_ROOT / "notebooks"


def lines(text: str):
    return text.splitlines(keepends=True)


def load_notebook(name: str):
    path = NOTEBOOKS_PATH / name
    return path, json.loads(path.read_text(encoding="utf-8-sig"))


def save_notebook(path: Path, notebook):
    for cell in notebook["cells"]:
        if cell.get("cell_type") == "code":
            cell["execution_count"] = None
            cell["outputs"] = []
    path.write_text(
        json.dumps(notebook, indent=1, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def set_cell(notebook, index: int, text: str):
    notebook["cells"][index]["source"] = lines(text)


def replace_in_cells(notebook, old: str, new: str):
    for cell in notebook["cells"]:
        source = "".join(cell.get("source", []))
        if old in source:
            cell["source"] = lines(source.replace(old, new))


def update_pipeline_notebook():
    path, notebook = load_notebook("01_Bronze_Pipeline_Parcial.ipynb")
    set_cell(
        notebook,
        0,
        """# Bronze pipeline del parcial

Este notebook explica el pipeline de ingesta Bronze del parcial para las tres fuentes: `ingresos`, `sismepre` y `renamu`. El flujo actualizado aterriza archivos fuente en `data/raw`, transforma tablas a Parquet en `data/bronze` y registra trazabilidad y calidad en `data/audit`.

## Contrato de capas

- `data/raw`: CSV, JSON, ZIP y PDF originales.
- `data/bronze`: exclusivamente Parquet y archivos técnicos de Spark.
- `data/audit`: ejecuciones, errores, métricas y verificaciones de calidad.
- Cada fila Bronze conserva `_bronze_source_url`, `_bronze_source_checksum`, `_bronze_execution_id` y `_bronze_ingestion_date`.
""",
    )
    set_cell(
        notebook,
        10,
        """bronze_rows = []
for file_path in sorted(BRONZE_PATH.rglob('*.parquet')):
    relative_path = file_path.relative_to(BRONZE_PATH)
    bronze_rows.append({
        'dataset': relative_path.parts[0],
        'parquet_file': str(relative_path),
        'size_mb': round(file_path.stat().st_size / (1024 * 1024), 2),
        'modified': pd.Timestamp(file_path.stat().st_mtime, unit='s')
    })

bronze_df = pd.DataFrame(bronze_rows).sort_values(['dataset', 'parquet_file']).reset_index(drop=True)
bronze_df
""",
    )
    set_cell(
        notebook,
        3,
        """resource_rows = []
for dataset_name, dataset_config in config['datasets'].items():
    if dataset_name == 'ingresos':
        resource_rows.append({'dataset': dataset_name, 'group': 'diccionario', 'filename': dataset_config['diccionario']['filename'], 'url': dataset_config['diccionario']['url']})
        for item in dataset_config.get('historico', []):
            resource_rows.append({'dataset': dataset_name, 'group': 'historico', 'filename': item['filename'], 'url': item['url']})
        for item in dataset_config.get('api', []):
            resource_rows.append({'dataset': dataset_name, 'group': 'api', 'filename': item['filename'], 'url': item['url']})
    elif dataset_name == 'sismepre':
        for group_name in ['diccionarios', 'archivos']:
            for item in dataset_config.get(group_name, []):
                resource_rows.append({'dataset': dataset_name, 'group': group_name, 'filename': item['filename'], 'url': item['url']})
    elif dataset_name == 'renamu':
        for group_name in ['diccionario', 'data_completa']:
            item = dataset_config[group_name]
            resource_rows.append({'dataset': dataset_name, 'group': group_name, 'filename': item['filename'], 'url': item['url']})

resources_df = pd.DataFrame(resource_rows)
resources_df
""",
    )
    set_cell(
        notebook,
        11,
        """bronze_df.groupby('dataset').agg(
    parquet_files=('parquet_file', 'count'),
    total_size_mb=('size_mb', 'sum')
).reset_index()
""",
    )
    set_cell(
        notebook,
        17,
        """## Conclusiones

- El pipeline actualizado aterriza las fuentes originales en `data/raw`.
- Bronze queda reservado para Parquet particionado y archivos técnicos de Spark.
- La auditoría más reciente se obtiene dinámicamente desde `data/audit`, por lo que este notebook no depende de un identificador de ejecución fijo.
- El pipeline evalúa ocho dimensiones: completitud, unicidad, validez, consistencia, integridad, actualidad, disponibilidad y exactitud.
- El siguiente paso es construir Silver con tipado, limpieza, deduplicación y cuarentena para las reglas fallidas de SISMEPRE.
""",
    )
    save_notebook(path, notebook)


def update_ingresos_notebook():
    path, notebook = load_notebook("02_Profiling_Ingresos.ipynb")
    replace_in_cells(
        notebook,
        "- Diccionario de datos `Ingresos_Diccionario.csv`",
        "- Diccionario original `Ingresos_Diccionario.csv` en Raw y referencia Parquet en Bronze",
    )
    set_cell(
        notebook,
        2,
        """## 1. Carga del dataset

En esta primera etapa se leen las particiones Parquet de ingresos disponibles en Bronze y se consolidan en un solo dataframe. Los CSV originales permanecen en `data/raw`. La columna auxiliar `source_file` se deriva de `_bronze_source_path` para facilitar la lectura humana sin perder la trazabilidad técnica.
""",
    )
    set_cell(
        notebook,
        3,
        """data_files = sorted(INGRESOS_PATH.glob('year=*'))
dictionary_path = INGRESOS_PATH / '_references' / 'ingresos_diccionario'

def read_ingresos_partition(partition_path: Path, key_columns: list[str]) -> pd.DataFrame:
    frame = pd.read_parquet(partition_path)
    selected_columns = [
        column for column in key_columns + [
            '_bronze_source_path', '_bronze_source_url', '_bronze_source_checksum',
            '_bronze_execution_id', '_bronze_ingestion_date'
        ]
        if column in frame.columns
    ]
    frame = frame[selected_columns].copy()
    frame['source_file'] = frame['_bronze_source_path'].map(lambda value: Path(value).name)
    return frame

ingresos_frames = [read_ingresos_partition(file_path, KEY_COLUMNS) for file_path in data_files]
ingresos_df = pd.concat(ingresos_frames, ignore_index=True)

for column in ['ANO_DOC', 'MES_DOC', 'MONTO_PIA', 'MONTO_PIM', 'MONTO_RECAUDADO']:
    ingresos_df[column] = pd.to_numeric(ingresos_df[column], errors='coerce')

for column in ['SEC_EJEC', 'PLIEGO', 'EJECUTORA', 'FUENTE_FINANCIAMIENTO', 'RUBRO', 'GENERICA', 'SUBGENERICA', 'ESPECIFICA', 'ESPECIFICA_DET', 'TIPO_RECURSO']:
    ingresos_df[column] = ingresos_df[column].astype('string')

dictionary_df = pd.read_parquet(dictionary_path) if dictionary_path.exists() else pd.DataFrame()

file_inventory = pd.DataFrame({
    'partition': [path.name for path in data_files],
    'records_loaded': [len(frame) for frame in ingresos_frames]
})

print(f'Particiones Parquet leidas: {len(data_files)}')
print(f'Diccionario Parquet disponible: {dictionary_path.exists()}')
print(f'Registros consolidados: {len(ingresos_df):,}'.replace(',', '.'))
display(file_inventory)
""",
    )
    replace_in_cells(
        notebook,
        "traceability_ratio = ingresos_df['source_file'].notna().mean()",
        "availability_ratio = float(bool(data_files) and not ingresos_df.empty)",
    )
    replace_in_cells(
        notebook,
        "ingresos_df['NIVEL_GOBIERNO'].isin(['N', 'R', 'L']).mean()",
        "ingresos_df['NIVEL_GOBIERNO'].isin(['E', 'R', 'M']).mean()",
    )
    replace_in_cells(
        notebook,
        "{'dimension': 'Trazabilidad', 'metric': 'presencia de source_file', 'score': round(traceability_ratio, 4)},",
        "{'dimension': 'Disponibilidad', 'metric': 'particiones Parquet Bronze disponibles y no vacias', 'score': round(availability_ratio, 4)},",
    )
    set_cell(
        notebook,
        33,
        """### 4.8 Disponibilidad

**Que significa:** comprobar que las particiones Parquet Bronze necesarias existen y contienen registros.

**Como se midio:** se verifico que exista al menos una particion `year=*` y que el dataframe consolidado no este vacio.

La trazabilidad se documenta adicionalmente con los metadatos técnicos `_bronze_*`.
""",
    )
    set_cell(
        notebook,
        34,
        """trazabilidad_detail = ingresos_df[
    ['source_file', '_bronze_source_url', '_bronze_source_checksum', '_bronze_execution_id', '_bronze_ingestion_date']
].drop_duplicates().sort_values('source_file')
display(quality_summary[quality_summary['dimension'] == 'Disponibilidad'])
display(trazabilidad_detail)
""",
    )
    replace_in_cells(
        notebook,
        "- Conservar `source_file` en capas posteriores para no perder trazabilidad.",
        "- Conservar los metadatos `_bronze_*` en Silver para mantener la trazabilidad técnica.",
    )
    save_notebook(path, notebook)


def update_sismepre_notebook():
    path, notebook = load_notebook("03_Profiling_SISMEPRE.ipynb")
    set_cell(
        notebook,
        2,
        """## 1. Carga de tablas

SISMEPRE se analiza desde las tablas Parquet de Bronze. Los CSV originales permanecen en `data/raw`. Se conserva una columna auxiliar `source_file`, derivada de `_bronze_source_path`, y los metadatos técnicos `_bronze_*`.
""",
    )
    replace_in_cells(
        notebook,
        "`rentas_respuestas.csv`",
        "`rentas_respuestas`",
    )
    replace_in_cells(
        notebook,
        "`rentas_ano_aplicacion.csv`",
        "`rentas_ano_aplicacion`",
    )
    replace_in_cells(
        notebook,
        "rentas_respuestas.csv con",
        "rentas_respuestas con",
    )
    replace_in_cells(
        notebook,
        "missing_files = availability_df.loc[~availability_df['available_in_bronze'], 'filename'].tolist()",
        "missing_tables = availability_df.loc[~availability_df['available_in_bronze'], 'table'].tolist()",
    )
    replace_in_cells(
        notebook,
        """    f'- Hay {int(availability_df["available_in_bronze"].sum())} archivos disponibles de {len(expected_files)} esperados.',
    f'- Los archivos faltantes son: {missing_files if missing_files else "ninguno"}.',
""",
        """    f'- Hay {int(availability_df["available_in_bronze"].sum())} tablas Parquet disponibles de {len(expected_tables)} esperadas.',
    f'- Las tablas faltantes son: {missing_tables if missing_tables else "ninguna"}.',
""",
    )
    replace_in_cells(
        notebook,
        "- SISMEPRE es util porque sus respuestas pueden relacionarse con preguntas y formularios, no es solo un CSV aislado.",
        "- SISMEPRE es util porque sus respuestas pueden relacionarse con preguntas y formularios, no es una tabla aislada.",
    )
    set_cell(
        notebook,
        3,
        """expected_tables = [
    'rentas_preguntas', 'rentas_estadistica', 'rentas_formulario',
    'rentas_esat_estadistica_atm', 'rentas_respuestas', 'rentas_ano_aplicacion',
    'rentas_entidad_estado'
]

availability_df = pd.DataFrame({
    'table': expected_tables,
    'available_in_bronze': [(SISMEPRE_PATH / table_name).exists() for table_name in expected_tables]
})

def read_parquet_if_exists(table_name: str) -> pd.DataFrame:
    table_path = SISMEPRE_PATH / table_name
    if not table_path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(table_path)
    if '_bronze_source_path' in frame.columns:
        frame['source_file'] = frame['_bronze_source_path'].map(lambda value: Path(value).name)
    return frame

tables = {
    'preguntas': read_parquet_if_exists('rentas_preguntas'),
    'estadistica': read_parquet_if_exists('rentas_estadistica'),
    'formulario': read_parquet_if_exists('rentas_formulario'),
    'esat_estadistica_atm': read_parquet_if_exists('rentas_esat_estadistica_atm'),
    'respuestas': read_parquet_if_exists('rentas_respuestas'),
    'ano_aplicacion': read_parquet_if_exists('rentas_ano_aplicacion'),
    'entidad_estado': read_parquet_if_exists('rentas_entidad_estado'),
}

respuestas_df = tables['respuestas'].copy()
preguntas_df = tables['preguntas'].copy()
formulario_df = tables['formulario'].copy()
ano_aplicacion_df = tables['ano_aplicacion'].copy()

for column in ['RESPUESTA_ID', 'PREGUNTA_ID', 'FORMULARIO_ID', 'ANO_APLICACION', 'PERIODO', 'SEC_EJEC', 'RESPUESTA_ENTERO', 'RESPUESTA_DECIMAL']:
    if column in respuestas_df.columns:
        respuestas_df[column] = pd.to_numeric(respuestas_df[column], errors='coerce')

for frame in [preguntas_df, formulario_df, ano_aplicacion_df]:
    for column in ['PREGUNTA_ID', 'FORMULARIO_ID', 'ANO_APLICACION', 'PERIODO']:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors='coerce')

table_shapes = pd.DataFrame([
    {'table': table_name, 'rows': frame.shape[0], 'columns': frame.shape[1]}
    for table_name, frame in tables.items()
])

print(f'Tablas esperadas: {len(expected_tables)}')
print(f'Tablas Parquet disponibles: {int(availability_df["available_in_bronze"].sum())}')
display(availability_df)
display(table_shapes)
""",
    )
    replace_in_cells(
        notebook,
        "trazabilidad_ratio = respuestas_df['source_file'].notna().mean()",
        "availability_ratio = float(availability_df['available_in_bronze'].all() and not respuestas_df.empty)",
    )
    replace_in_cells(
        notebook,
        "{'dimension': 'Trazabilidad', 'metric': 'presencia de source_file', 'score': round(trazabilidad_ratio, 4)},",
        "{'dimension': 'Disponibilidad', 'metric': 'tablas Parquet Bronze esperadas y tabla principal no vacia', 'score': round(availability_ratio, 4)},",
    )
    set_cell(
        notebook,
        33,
        """### 4.8 Disponibilidad

**Que significa:** confirmar que las tablas Parquet necesarias para analizar SISMEPRE existen en Bronze.

**Como se midio:** se verifico la existencia de las siete tablas principales y que `rentas_respuestas` contenga registros.

La trazabilidad se documenta adicionalmente con los metadatos técnicos `_bronze_*`.
""",
    )
    set_cell(
        notebook,
        34,
        """trazabilidad_detail = respuestas_df[
    ['source_file', '_bronze_source_url', '_bronze_source_checksum', '_bronze_execution_id', '_bronze_ingestion_date']
].drop_duplicates().sort_values('source_file')
display(quality_summary[quality_summary['dimension'] == 'Disponibilidad'])
display(availability_df)
display(trazabilidad_detail)
""",
    )
    save_notebook(path, notebook)


def update_renamu_notebook():
    path, notebook = load_notebook("04_Profiling_RENAMU.ipynb")
    replace_in_cells(
        notebook,
        "RENAMU_FILE = RENAMU_PATH / 'Base_RENAMU_2022_f.csv'",
        "RENAMU_TABLE_PATH = RENAMU_PATH / 'year=2022'",
    )
    replace_in_cells(
        notebook,
        "- Archivo `Base_RENAMU_2022_f.csv` en `data/bronze/renamu`",
        "- Partición Parquet `year=2022` en `data/bronze/renamu`",
    )
    set_cell(
        notebook,
        2,
        """## 1. Carga del dataset

RENAMU se analiza desde la partición Parquet Bronze `year=2022`. El CSV y el ZIP originales permanecen en `data/raw`. Se conserva una columna auxiliar `source_file`, derivada de `_bronze_source_path`, junto con los metadatos técnicos `_bronze_*`.
""",
    )
    set_cell(
        notebook,
        3,
        """renamu_df = pd.read_parquet(RENAMU_TABLE_PATH)
renamu_df['source_file'] = renamu_df['_bronze_source_path'].map(lambda value: Path(value).name)

key_columns = [
    'Año', 'idmunici', 'ccdd', 'ccpp', 'ccdi', 'Ubigeo', 'Departamento',
    'Provincia', 'Distrito', 'Tipomuni', 'source_file', '_bronze_source_url',
    '_bronze_source_checksum', '_bronze_execution_id', '_bronze_ingestion_date'
]
renamu_key_df = renamu_df[key_columns].copy()

for column in ['Año', 'Tipomuni']:
    renamu_key_df[column] = pd.to_numeric(renamu_key_df[column], errors='coerce')

for column in ['idmunici', 'ccdd', 'ccpp', 'ccdi', 'Ubigeo']:
    renamu_key_df[column] = renamu_key_df[column].astype('string').str.replace('.0', '', regex=False).str.strip()

print(f'Particion principal: {RENAMU_TABLE_PATH.name}')
print(f'Dimension de la tabla completa: {renamu_df.shape[0]:,} filas x {renamu_df.shape[1]:,} columnas'.replace(',', '.'))
print(f'Dimension del subconjunto clave: {renamu_key_df.shape[0]:,} filas x {renamu_key_df.shape[1]:,} columnas'.replace(',', '.'))
""",
    )
    replace_in_cells(
        notebook,
        "trazabilidad_ratio = renamu_key_df['source_file'].notna().mean()",
        "availability_ratio = float(RENAMU_TABLE_PATH.exists() and not renamu_key_df.empty)",
    )
    replace_in_cells(
        notebook,
        "renamu_key_df['Tipomuni'].dropna().isin([1, 2, 3]).mean()",
        "renamu_key_df['Tipomuni'].dropna().isin([1, 2]).mean()",
    )
    replace_in_cells(
        notebook,
        "{'dimension': 'Trazabilidad', 'metric': 'presencia de source_file', 'score': round(trazabilidad_ratio, 4)},",
        "{'dimension': 'Disponibilidad', 'metric': 'particion Parquet Bronze disponible y no vacia', 'score': round(availability_ratio, 4)},",
    )
    set_cell(
        notebook,
        33,
        """### 4.8 Disponibilidad

**Que significa:** comprobar que la partición Parquet Bronze de RENAMU existe y contiene registros.

**Como se midio:** se verifico la disponibilidad de `year=2022` y que el dataframe no este vacio.

La trazabilidad se documenta adicionalmente con los metadatos técnicos `_bronze_*`.
""",
    )
    set_cell(
        notebook,
        34,
        """trazabilidad_detail = renamu_key_df[
    ['source_file', '_bronze_source_url', '_bronze_source_checksum', '_bronze_execution_id', '_bronze_ingestion_date']
].drop_duplicates().sort_values('source_file')
display(quality_summary[quality_summary['dimension'] == 'Disponibilidad'])
display(trazabilidad_detail)
""",
    )
    replace_in_cells(
        notebook,
        "- Mantener `idmunici` y `source_file` como columnas de apoyo para trazabilidad y validacion.",
        "- Mantener `idmunici` y los metadatos `_bronze_*` como apoyo para trazabilidad y validacion.",
    )
    save_notebook(path, notebook)


def main():
    update_pipeline_notebook()
    update_ingresos_notebook()
    update_sismepre_notebook()
    update_renamu_notebook()


if __name__ == "__main__":
    main()
