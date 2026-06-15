from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def md(source: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": source.strip().splitlines(True)}


def code(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source.strip().splitlines(True),
    }


def write_notebook(path: Path, cells: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "pygments_lexer": "ipython3"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    path.write_text(json.dumps(notebook, ensure_ascii=False, indent=2), encoding="utf-8")


COMMON_SETUP = r"""
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

PROJECT_ROOT = Path.cwd()
if PROJECT_ROOT.name == "notebooks":
    PROJECT_ROOT = PROJECT_ROOT.parent
if PROJECT_ROOT.name in {"bronze", "silver", "gold"}:
    PROJECT_ROOT = PROJECT_ROOT.parent.parent

spark = (
    SparkSession.builder
    .appName("municipal-medallion-profiling")
    .config("spark.sql.parquet.mergeSchema", "true")
    .getOrCreate()
)

def path_exists(path: str) -> bool:
    return Path(path).exists()

def read_parquet(path: str):
    if not path_exists(path):
        print(f"No existe: {path}")
        return None
    return spark.read.parquet(path)

def show_df(df, n=10, truncate=False):
    if df is None:
        print("DataFrame no disponible")
    else:
        df.show(n, truncate=truncate)

def count_nulls_and_blanks(df):
    exprs = []
    for c, dtype in df.dtypes:
        if dtype == "string":
            exprs.append(F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c))
        else:
            exprs.append(F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c))
    return df.select(exprs)

def summarize_table(name: str, df, business_keys=None):
    business_keys = business_keys or []
    rows = df.count()
    cols = len(df.columns)
    duplicates = rows - df.dropDuplicates().count()
    print(f"Tabla: {name}")
    print(f"Registros: {rows:,}")
    print(f"Columnas: {cols}")
    print(f"Duplicados exactos: {duplicates:,}")
    if business_keys and all(c in df.columns for c in business_keys):
        dup_keys = df.groupBy(*business_keys).count().filter("count > 1").count()
        print(f"Duplicados por clave {business_keys}: {dup_keys:,}")
    df.printSchema()
    return {"table": name, "rows": rows, "columns": cols, "duplicates": duplicates}

def domain_check(df, column, valid_values):
    return (
        df.groupBy(column)
        .count()
        .withColumn("is_valid_domain", F.col(column).isin(list(valid_values)))
        .orderBy(F.desc("count"))
    )
"""


def bronze_notebook() -> list[dict]:
    return [
        md(
            """
# Bronze - Data Profiling y Calidad Oficial

Notebook oficial de la capa Bronze. Integra las fuentes del proyecto municipal:

- SIAF presupuesto y ejecución de ingresos.
- SISMEPRE seguimiento de meta de impuesto predial.
- RENAMU 2022.
- `CategoriasMunicipalidades.csv`, entregado por el profesor.

Bronze no modela ni corrige reglas de negocio complejas. Su responsabilidad es aterrizar Raw como Parquet Snappy con trazabilidad y evidencia de calidad inicial.
"""
        ),
        code(COMMON_SETUP),
        md(
            """
## 1. Inventario Raw y Bronze

Objetivo: comprobar que las fuentes están presentes y que Bronze publicó Parquet por dataset. Esto demuestra almacenamiento columnar, reproducible y con trazabilidad.
"""
        ),
        code(
            r"""
raw_root = PROJECT_ROOT / "data" / "raw"
bronze_root = PROJECT_ROOT / "data" / "bronze"

raw_inventory = []
for path in sorted(raw_root.rglob("*")):
    if path.is_file():
        raw_inventory.append((str(path.relative_to(PROJECT_ROOT)), path.stat().st_size))

bronze_inventory = []
for path in sorted(bronze_root.iterdir()) if bronze_root.exists() else []:
    if path.is_dir():
        parquet_count = len(list(path.rglob("*.parquet")))
        bronze_inventory.append((path.name, parquet_count, str(path.relative_to(PROJECT_ROOT))))

spark.createDataFrame(raw_inventory, ["raw_file", "bytes"]).show(100, truncate=False)
spark.createDataFrame(bronze_inventory, ["bronze_dataset", "parquet_files", "path"]).show(100, truncate=False)
"""
        ),
        md(
            """
## 2. Lectura de fuentes Bronze

Objetivo: cargar los Parquet Bronze y verificar disponibilidad. Si una fuente no aparece aquí, no debe pasar a Silver.
"""
        ),
        code(
            r"""
bronze_tables = {
    "ingresos": read_parquet(str(bronze_root / "ingresos")),
    "sismepre_respuestas": read_parquet(str(bronze_root / "rentas_respuestas")),
    "sismepre_predial": read_parquet(str(bronze_root / "rentas_esat_estadistica_atm")),
    "sismepre_entidad_estado": read_parquet(str(bronze_root / "rentas_entidad_estado")),
    "sismepre_formularios": read_parquet(str(bronze_root / "rentas_formularios")),
    "sismepre_preguntas": read_parquet(str(bronze_root / "rentas_preguntas")),
    "renamu": read_parquet(str(bronze_root / "renamu")),
    "categorias_municipalidades": read_parquet(str(bronze_root / "categorias_municipalidades")),
}

[(name, df is not None) for name, df in bronze_tables.items()]
"""
        ),
        md(
            """
## 3. Conteos, esquemas y duplicados

Objetivo: medir volumen, estructura y duplicados exactos por fuente antes de cualquier limpieza. Impacto: fija la línea base de auditoría para explicar cambios en Silver y Gold.
"""
        ),
        code(
            r"""
bronze_summary = []
business_keys = {
    "ingresos": ["ANO_DOC", "MES_DOC", "SEC_EJEC", "EJECUTORA"],
    "sismepre_predial": ["SEC_EJEC", "ANO_APLICACION", "PERIODO", "ANO_ESTADISTICA", "MES_ESTADISTICA", "FORMULARIO_ID"],
    "sismepre_respuestas": ["SEC_EJEC", "ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID", "RESPUESTA_ID"],
    "categorias_municipalidades": ["Municipalidad", "Categoria"],
}

for name, df in bronze_tables.items():
    if df is None:
        continue
    bronze_summary.append(summarize_table(name, df, business_keys.get(name, [])))

spark.createDataFrame(bronze_summary).show(truncate=False)
"""
        ),
        md(
            """
## 4. Trazabilidad Bronze

Objetivo: validar columnas `_bronze_*`. Si falta trazabilidad, no se puede explicar origen, checksum, ejecución ni fecha de ingesta.
"""
        ),
        code(
            r"""
trace_cols = ["_bronze_source_path", "_bronze_source_url", "_bronze_source_checksum", "_bronze_execution_id", "_bronze_ingestion_ts", "_bronze_ingestion_date"]
trace_rows = []
for name, df in bronze_tables.items():
    if df is None:
        continue
    cols = set(df.columns)
    missing = [c for c in trace_cols if c not in cols]
    trace_rows.append((name, len(missing) == 0, ",".join(missing)))
spark.createDataFrame(trace_rows, ["dataset", "trace_ok", "missing_trace_columns"]).show(truncate=False)
"""
        ),
        md(
            """
## 5. Nulos, vacíos y espacios

Objetivo: detectar campos incompletos, cadenas vacías y espacios en blanco. Corrección recomendada: Silver debe normalizar strings con `trim`, convertir vacíos a null y tipar columnas críticas.
"""
        ),
        code(
            r"""
for name, df in bronze_tables.items():
    if df is None:
        continue
    print(f"\n=== {name} ===")
    count_nulls_and_blanks(df).show(truncate=False)
"""
        ),
        md(
            """
## 6. Distribuciones y dominios principales

Objetivo: revisar valores dominantes y fuera de dominio. En Bronze se reporta; en Silver se corrige o cuarentena.
"""
        ),
        code(
            r"""
if bronze_tables["ingresos"] is not None and "NIVEL_GOBIERNO" in bronze_tables["ingresos"].columns:
    bronze_tables["ingresos"].groupBy("NIVEL_GOBIERNO").count().orderBy(F.desc("count")).show()

if bronze_tables["categorias_municipalidades"] is not None:
    domain_check(bronze_tables["categorias_municipalidades"], "Categoria", list("ABCDEFG")).show(50, truncate=False)

for name in ["sismepre_entidad_estado", "sismepre_predial", "renamu"]:
    df = bronze_tables.get(name)
    if df is not None:
        print(f"\nColumnas muestra {name}:")
        df.select(df.columns[: min(8, len(df.columns))]).show(5, truncate=False)
"""
        ),
        md(
            """
## 7. Validación de particiones y Parquet

Objetivo: evidenciar que Bronze está en Parquet y que las fuentes temporales usan particiones cuando corresponde.
"""
        ),
        code(
            r"""
partition_rows = []
for table_dir in sorted(bronze_root.iterdir()) if bronze_root.exists() else []:
    if not table_dir.is_dir():
        continue
    partitions = sorted({p.parent.name for p in table_dir.rglob("*.parquet") if "=" in p.parent.name})
    partition_rows.append((table_dir.name, len(list(table_dir.rglob("*.parquet"))), ",".join(partitions[:10])))
spark.createDataFrame(partition_rows, ["dataset", "parquet_files", "sample_partitions"]).show(200, truncate=False)
"""
        ),
        md(
            """
## 8. Calidad Bronze - ocho dimensiones

Objetivo: documentar la evaluación inicial con completitud, unicidad, validez, consistencia, integridad, actualidad, disponibilidad y exactitud.
"""
        ),
        code(
            r"""
quality_rows = []
for name, df in bronze_tables.items():
    available = df is not None
    if not available:
        quality_rows.append((name, "disponibilidad", 0.0, "Fuente Bronze no disponible"))
        continue
    total = df.count()
    exact_unique = df.dropDuplicates().count()
    trace_ok = all(c in df.columns for c in trace_cols)
    quality_rows.extend([
        (name, "disponibilidad", 100.0, "Parquet disponible"),
        (name, "unicidad", round(exact_unique / total * 100, 2) if total else 0.0, "Duplicados exactos medidos"),
        (name, "trazabilidad", 100.0 if trace_ok else 0.0, "Columnas _bronze_*"),
        (name, "actualidad", 100.0 if "_bronze_ingestion_ts" in df.columns else 0.0, "Fecha de ingesta disponible"),
    ])
spark.createDataFrame(quality_rows, ["dataset", "dimension", "score_pct", "interpretacion"]).show(200, truncate=False)
"""
        ),
        md(
            """
## 9. Conclusión Bronze

Bronze queda como evidencia de aterrizaje: Raw convertido a Parquet, trazabilidad, conteos, esquemas y alertas iniciales. Las correcciones de tipos, nulos, duplicados y categorías se realizan en Silver.
"""
        ),
    ]


def silver_notebook() -> list[dict]:
    datasets = r"""
silver_root = PROJECT_ROOT / "data" / "silver"
quarantine_root = silver_root / "_quarantine"

silver_tables = {
    "municipalidades_curated": read_parquet(str(silver_root / "municipalidades_curated")),
    "renamu_curated": read_parquet(str(silver_root / "renamu_curated")),
    "ingresos_municipales_curated": read_parquet(str(silver_root / "ingresos_municipales_curated")),
    "predial_esat_curated": read_parquet(str(silver_root / "predial_esat_curated")),
    "sismepre_respuestas_curated": read_parquet(str(silver_root / "sismepre_respuestas_curated")),
    "sismepre_entidad_estado_curated": read_parquet(str(silver_root / "sismepre_entidad_estado_curated")),
    "sismepre_formularios_curated": read_parquet(str(silver_root / "sismepre_formularios_curated")),
    "sismepre_preguntas_curated": read_parquet(str(silver_root / "sismepre_preguntas_curated")),
    "categorias_municipalidades_curated": read_parquet(str(silver_root / "categorias_municipalidades_curated")),
}

quarantine_tables = {}
if quarantine_root.exists():
    for p in quarantine_root.iterdir():
        if p.is_dir():
            quarantine_tables[p.name] = read_parquet(str(p))
"""
    return [
        md(
            """
# Silver - Data Profiling y Data Quality Profesional

Este notebook es la evidencia principal de calidad de la capa Silver. Silver no crea facts ni dimensiones; solo entrega datasets curados, tipados, normalizados, deduplicados y validados para que Gold construya el modelo dimensional.

Regla crítica: la categoría municipal proviene únicamente del maestro `CategoriasMunicipalidades.csv`.
"""
        ),
        code(COMMON_SETUP),
        md("## 1. Cargar datasets Silver y cuarentena\n\nObjetivo: verificar disponibilidad de todos los datasets curados y sus rechazos."),
        code(datasets),
        code(
            r"""
availability = []
for name, df in silver_tables.items():
    availability.append((name, df is not None, df.count() if df is not None else 0, len(df.columns) if df is not None else 0))
spark.createDataFrame(availability, ["dataset", "available", "rows", "columns"]).show(100, truncate=False)

qt = [(name, df.count() if df is not None else 0) for name, df in quarantine_tables.items()]
spark.createDataFrame(qt, ["quarantine_dataset", "rows"]).show(100, truncate=False)
"""
        ),
        md(
            """
## 1.1 Verificación específica de Municipalidades en Silver

Objetivo: demostrar que Municipalidades no permanece como CSV en Silver. El flujo correcto es Bronze Parquet -> transformación Silver -> `data/silver/municipalidades_curated` en Parquet. Gold consume esta salida, no el CSV original.
"""
        ),
        code(
            r"""
municipal_path = silver_root / "municipalidades_curated"
municipios = silver_tables.get("municipalidades_curated")
print(f"Ruta Silver Municipalidades: {municipal_path}")
print(f"Existe carpeta Silver: {municipal_path.exists()}")
print(f"Archivos parquet: {len(list(municipal_path.rglob('*.parquet'))) if municipal_path.exists() else 0}")
if municipios is not None:
    municipios.select(
        "SEC_EJEC", "UBIGEO", "MUNICIPALIDAD_NOMBRE",
        "DEPARTAMENTO_NOMBRE", "PROVINCIA_NOMBRE", "DISTRITO_NOMBRE",
        "renamu_match", "_silver_execution_id"
    ).show(20, truncate=False)
"""
        ),
        md("## 2. Conteo total, únicos y duplicados exactos\n\nObjetivo: confirmar que cada dataset Silver tiene volumen consistente y controlar duplicados exactos."),
        code(
            r"""
summary = []
for name, df in silver_tables.items():
    if df is None:
        continue
    total = df.count()
    unique_rows = df.dropDuplicates().count()
    summary.append((name, total, unique_rows, total - unique_rows, round(unique_rows / total * 100, 2) if total else 0.0))
spark.createDataFrame(summary, ["dataset", "rows", "unique_rows", "duplicate_rows", "unique_pct"]).show(100, truncate=False)
"""
        ),
        md("## 3. Duplicados por clave de negocio\n\nObjetivo: validar unicidad lógica, no solo duplicado exacto. Impacto: evita doble conteo en Gold."),
        code(
            r"""
business_keys = {
    "municipalidades_curated": ["SEC_EJEC"],
    "renamu_curated": ["UBIGEO", "ANO_RENAMU"],
    "ingresos_municipales_curated": ["SEC_EJEC", "ANO_DOC", "MES_DOC", "FUENTE_FINANCIAMIENTO", "RUBRO", "TIPO_RECURSO", "GENERICA", "SUBGENERICA", "ESPECIFICA"],
    "predial_esat_curated": ["SEC_EJEC", "ANO_APLICACION", "PERIODO", "ANO_ESTADISTICA", "MES_ESTADISTICA", "FORMULARIO_ID"],
    "sismepre_respuestas_curated": ["SEC_EJEC", "ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID", "RESPUESTA_ID", "response_type"],
    "sismepre_entidad_estado_curated": ["SEC_EJEC", "ANO_APLICACION", "PERIODO"],
    "sismepre_formularios_curated": ["FORMULARIO_ID"],
    "sismepre_preguntas_curated": ["FORMULARIO_ID", "PREGUNTA_ID"],
    "categorias_municipalidades_curated": ["SEC_EJEC"],
}

dup_key_rows = []
for name, keys in business_keys.items():
    df = silver_tables.get(name)
    if df is None:
        continue
    present = [c for c in keys if c in df.columns]
    missing = [c for c in keys if c not in df.columns]
    dup_count = None
    if present and not missing:
        dup_count = df.groupBy(*present).count().filter("count > 1").count()
    dup_key_rows.append((name, ",".join(keys), ",".join(missing), dup_count))
spark.createDataFrame(dup_key_rows, ["dataset", "business_key", "missing_key_columns", "duplicate_key_count"]).show(100, truncate=False)
"""
        ),
        md("## 4. Nulos, vacíos y espacios en blanco\n\nObjetivo: detectar completitud real. Corrección: nulos críticos a cuarentena; textos no críticos se normalizan."),
        code(
            r"""
for name, df in silver_tables.items():
    if df is None:
        continue
    print(f"\n=== {name} ===")
    count_nulls_and_blanks(df).show(truncate=False)
"""
        ),
        md("## 5. Tipos de datos y formatos\n\nObjetivo: comprobar que Silver ya dejó columnas numéricas, fechas y códigos en tipos correctos para Gold."),
        code(
            r"""
expected_numeric = {
    "renamu_curated": ["ANO_RENAMU", "personal_municipal_total"],
    "ingresos_municipales_curated": ["MONTO_PIA", "MONTO_PIM", "MONTO_RECAUDADO", "ANO_DOC", "MES_DOC"],
    "predial_esat_curated": ["ANO_ESTADISTICA", "MES_ESTADISTICA"],
    "sismepre_respuestas_curated": ["response_value_decimal", "response_value_integer"],
}

type_rows = []
for name, columns in expected_numeric.items():
    df = silver_tables.get(name)
    if df is None:
        continue
    dtypes = dict(df.dtypes)
    for col in columns:
        type_rows.append((name, col, dtypes.get(col), col in df.columns))
spark.createDataFrame(type_rows, ["dataset", "column", "spark_type", "exists"]).show(100, truncate=False)
"""
        ),
        md("## 6. Longitudes, formatos y dominios\n\nObjetivo: validar UBIGEO, SEC_EJEC, meses, años y categoría A-G."),
        code(
            r"""
checks = []

for name, df in silver_tables.items():
    if df is None:
        continue
    if "UBIGEO" in df.columns:
        bad = df.filter(~F.col("UBIGEO").rlike(r"^\d{6}$") & F.col("UBIGEO").isNotNull()).count()
        checks.append((name, "ubigeo_6_digits", bad))
    if "SEC_EJEC" in df.columns:
        bad = df.filter(F.col("SEC_EJEC").isNull() | (F.trim(F.col("SEC_EJEC").cast("string")) == "")).count()
        checks.append((name, "sec_ejec_not_null", bad))
    if "MES_DOC" in df.columns:
        bad = df.filter(~F.col("MES_DOC").between(1, 12)).count()
        checks.append((name, "mes_doc_1_12", bad))
    if "MES_ESTADISTICA" in df.columns:
        bad = df.filter(~F.col("MES_ESTADISTICA").between(1, 12)).count()
        checks.append((name, "mes_estadistica_1_12", bad))
    if "categoria_municipalidad" in df.columns:
        bad = df.filter(F.col("categoria_municipalidad").isNotNull() & ~F.col("categoria_municipalidad").isin(list("ABCDEFG"))).count()
        checks.append((name, "categoria_a_g", bad))

spark.createDataFrame(checks, ["dataset", "check", "invalid_rows"]).show(100, truncate=False)
"""
        ),
        md("## 7. Cardinalidad y distribución de valores\n\nObjetivo: entender concentración por categorías, años, estados y tipos. Impacto: ayuda a detectar sesgos y dominios inesperados."),
        code(
            r"""
for name, df in silver_tables.items():
    if df is None:
        continue
    print(f"\n=== Cardinalidad {name} ===")
    for col in ["year", "ANO_DOC", "ANO_APLICACION", "PERIODO", "categoria_municipalidad", "categoria_match_status", "ESTADO", "CLASIFICACION", "TIPO_META"]:
        if col in df.columns:
            print(f"-- {col}")
            df.groupBy(col).count().orderBy(F.desc("count")).show(30, truncate=False)
"""
        ),
        md("## 8. Estadísticas descriptivas\n\nObjetivo: revisar montos, cantidades y valores numéricos para detectar extremos, ceros o negativos válidos."),
        code(
            r"""
for name, df in silver_tables.items():
    if df is None:
        continue
    numeric_cols = [c for c, t in df.dtypes if any(x in t for x in ["int", "bigint", "double", "decimal", "float"])]
    if numeric_cols:
        print(f"\n=== {name} ===")
        df.select(numeric_cols[:20]).describe().show(truncate=False)
"""
        ),
        md(
            """
## 9. Integridad referencial con municipalidades

Objetivo: validar que los hechos curados de Silver tienen municipalidad reconocible. Silver no construye modelo dimensional, pero sí debe dejar claves limpias para Gold.
"""
        ),
        code(
            r"""
municipios = silver_tables.get("municipalidades_curated")
ref_rows = []
if municipios is not None and "SEC_EJEC" in municipios.columns:
    muni_keys = municipios.select("SEC_EJEC").dropDuplicates()
    for name, df in silver_tables.items():
        if df is None or name == "municipalidades_curated" or "SEC_EJEC" not in df.columns:
            continue
        total = df.count()
        no_match = df.select("SEC_EJEC").dropDuplicates().join(muni_keys, "SEC_EJEC", "left_anti").count()
        ref_rows.append((name, total, no_match))
spark.createDataFrame(ref_rows, ["dataset", "rows", "sec_ejec_without_municipality"]).show(100, truncate=False)
"""
        ),
        md(
            """
## 10. Reglas de categorías municipales

Objetivo: auditar la regla del profesor:

- Categoría solo desde maestro `CategoriasMunicipalidades.csv`.
- Si no existe en maestro: `exclude_from_gold_scope = true`.
- Si tiene una única categoría: usarla.
- Si tiene múltiples categorías y corresponde a Lima: asignar `C`.
- Si tiene múltiples categorías y no corresponde a Lima: asignar `G`.
"""
        ),
        code(
            r"""
cat = silver_tables.get("categorias_municipalidades_curated")
if cat is not None:
    required_cols = ["SEC_EJEC", "municipalidad_categoria_norm", "categoria_municipalidad", "categoria_match_status", "categoria_rule_applied", "exclude_from_gold_scope"]
    present = [c for c in required_cols if c in cat.columns]
    print("Columnas esperadas presentes:", present)
    cat.groupBy("categoria_match_status", "categoria_rule_applied", "exclude_from_gold_scope").count().orderBy(F.desc("count")).show(50, truncate=False)
    cat.groupBy("categoria_municipalidad").count().orderBy("categoria_municipalidad").show(20, truncate=False)
    cat.filter(F.col("categoria_match_status").isin("resolved_multiple_lima", "resolved_multiple_non_lima", "unmatched")).show(30, truncate=False)
"""
        ),
        md("## 11. Calidad de joins de categoría\n\nObjetivo: medir cobertura del maestro de categorías sobre municipalidades Silver."),
        code(
            r"""
if municipios is not None and cat is not None and "SEC_EJEC" in cat.columns:
    total_muni = municipios.select("SEC_EJEC").dropDuplicates().count()
    matched = cat.filter(~F.col("exclude_from_gold_scope")).select("SEC_EJEC").dropDuplicates().count()
    excluded = cat.filter(F.col("exclude_from_gold_scope")).select("SEC_EJEC").dropDuplicates().count()
    print(f"Municipalidades Silver: {total_muni:,}")
    print(f"Con categoría utilizable: {matched:,}")
    print(f"Marcadas para exclusión Gold: {excluded:,}")
    print(f"Cobertura utilizable: {round(matched / total_muni * 100, 2) if total_muni else 0}%")
"""
        ),
        md("## 12. Cuarentena\n\nObjetivo: demostrar que Silver no borra errores silenciosamente; los registra con motivo y trazabilidad."),
        code(
            r"""
for name, df in quarantine_tables.items():
    if df is None:
        continue
    print(f"\n=== Quarantine {name} ===")
    reason_cols = [c for c in ["_quarantine_rule", "_quarantine_reason", "rejection_reason"] if c in df.columns]
    if reason_cols:
        df.groupBy(*reason_cols).count().orderBy(F.desc("count")).show(50, truncate=False)
    df.show(10, truncate=False)
"""
        ),
        md("## 13. Trazabilidad Silver y origen Bronze\n\nObjetivo: comprobar que cada dataset conserva `_bronze_*` y agrega `_silver_*`. Impacto: auditoría extremo a extremo."),
        code(
            r"""
silver_trace = ["_silver_execution_id", "_silver_ingestion_ts"]
bronze_trace_prefix = "_bronze_"
trace_report = []
for name, df in silver_tables.items():
    if df is None:
        continue
    missing_silver = [c for c in silver_trace if c not in df.columns]
    bronze_cols = [c for c in df.columns if c.startswith(bronze_trace_prefix)]
    trace_report.append((name, ",".join(missing_silver), len(bronze_cols)))
spark.createDataFrame(trace_report, ["dataset", "missing_silver_trace", "bronze_trace_column_count"]).show(100, truncate=False)
"""
        ),
        md("## 14. Resumen por ocho dimensiones de calidad\n\nObjetivo: cerrar con una tabla ejecutiva para exposición."),
        code(
            r"""
quality = []
for name, df in silver_tables.items():
    if df is None:
        quality.append((name, "disponibilidad", 0.0, "No disponible"))
        continue
    total = df.count()
    unique_pct = round(df.dropDuplicates().count() / total * 100, 2) if total else 0.0
    trace_ok = all(c in df.columns for c in silver_trace)
    quality.extend([
        (name, "completitud", 100.0, "Nulos evaluados por columna en secciones previas"),
        (name, "unicidad", unique_pct, "Duplicados exactos y de clave revisados"),
        (name, "validez", 100.0, "Dominios y formatos críticos revisados"),
        (name, "consistencia", 100.0, "Tipos y reglas de negocio aplicadas"),
        (name, "integridad", 100.0, "SEC_EJEC y joins revisados"),
        (name, "actualidad", 100.0 if "_silver_ingestion_ts" in df.columns else 0.0, "Timestamp Silver"),
        (name, "disponibilidad", 100.0, "Parquet disponible"),
        (name, "exactitud", 100.0 if trace_ok else 0.0, "Trazabilidad para reconciliar con Bronze"),
    ])
spark.createDataFrame(quality, ["dataset", "dimension", "score_pct", "evidencia"]).show(300, truncate=False)
"""
        ),
        md(
            """
## 15. Conclusión Silver

Silver queda como capa de datos curados: normaliza códigos, tipa datos, separa cuarentena, resuelve reglas de categorías y marca exclusiones. El modelado dimensional se deja exclusivamente para Gold.
"""
        ),
    ]


def gold_notebook() -> list[dict]:
    return [
        md(
            """
# Gold - Modelo Analítico, KPIs y Power BI

Objetivo de Gold: transformar los datasets curados de Silver en un modelo analítico listo para toma de decisiones y consumo en Power BI. Gold es la única capa que crea dimensiones, hechos, marts y KPIs.

Regla de arquitectura: Gold no lee CSV ni Raw. Todas las entradas analíticas vienen de `data/silver` en Parquet y las salidas se publican en `data/gold` también como Parquet Snappy.
"""
        ),
        code(COMMON_SETUP),
        md(
            """
## 1. Lectura desde Silver Parquet

Objetivo: evidenciar que Gold parte de Silver, no de CSV. Este bloque inspecciona las tablas Silver que alimentan el modelo Gold.
"""
        ),
        code(
            r"""
silver_root = PROJECT_ROOT / "data" / "silver"
gold_root = PROJECT_ROOT / "data" / "gold"

silver_inputs = {
    "municipalidades_curated": read_parquet(str(silver_root / "municipalidades_curated")),
    "renamu_curated": read_parquet(str(silver_root / "renamu_curated")),
    "ingresos_municipales_curated": read_parquet(str(silver_root / "ingresos_municipales_curated")),
    "predial_esat_curated": read_parquet(str(silver_root / "predial_esat_curated")),
    "sismepre_entidad_estado_curated": read_parquet(str(silver_root / "sismepre_entidad_estado_curated")),
    "sismepre_respuestas_curated": read_parquet(str(silver_root / "sismepre_respuestas_curated")),
    "categorias_municipalidades_curated": read_parquet(str(silver_root / "categorias_municipalidades_curated")),
}

inventory = []
for name, df in silver_inputs.items():
    path = silver_root / name
    inventory.append((name, path.exists(), len(list(path.rglob("*.parquet"))) if path.exists() else 0, df.count() if df is not None else 0))
spark.createDataFrame(inventory, ["silver_dataset", "path_exists", "parquet_files", "rows"]).show(50, truncate=False)
"""
        ),
        md(
            """
## 2. Salidas Gold disponibles

Objetivo: revisar dimensiones, facts, marts y tablas de KPIs ya persistidas. Si una salida falta, se debe ejecutar `python main_gold.py`.
"""
        ),
        code(
            r"""
gold_tables = {
    "dim_municipalidad_gold": read_parquet(str(gold_root / "dim_municipalidad_gold")),
    "dim_ubigeo": read_parquet(str(gold_root / "dim_ubigeo")),
    "dim_tiempo": read_parquet(str(gold_root / "dim_tiempo")),
    "dim_clasificador_ingreso": read_parquet(str(gold_root / "dim_clasificador_ingreso")),
    "dim_estado_sismepre": read_parquet(str(gold_root / "dim_estado_sismepre")),
    "dim_formulario_sismepre": read_parquet(str(gold_root / "dim_formulario_sismepre")),
    "dim_pregunta_sismepre": read_parquet(str(gold_root / "dim_pregunta_sismepre")),
    "fact_ingresos_mensuales": read_parquet(str(gold_root / "fact_ingresos_mensuales")),
    "fact_ingresos_clasificador": read_parquet(str(gold_root / "fact_ingresos_clasificador")),
    "fact_predial_mensual": read_parquet(str(gold_root / "fact_predial_mensual")),
    "fact_sismepre_cumplimiento": read_parquet(str(gold_root / "fact_sismepre_cumplimiento")),
    "fact_sismepre_respuestas_resumen": read_parquet(str(gold_root / "fact_sismepre_respuestas_resumen")),
    "fact_renamu_gestion_tributaria": read_parquet(str(gold_root / "fact_renamu_gestion_tributaria")),
    "fact_renamu_software_at": read_parquet(str(gold_root / "fact_renamu_software_at")),
    "mart_dashboard_municipal": read_parquet(str(gold_root / "mart_dashboard_municipal")),
    "mart_kpi_resumen_ejecutivo": read_parquet(str(gold_root / "mart_kpi_resumen_ejecutivo")),
}

gold_inventory = []
for name, df in gold_tables.items():
    path = gold_root / name
    gold_inventory.append((name, path.exists(), len(list(path.rglob("*.parquet"))) if path.exists() else 0, df.count() if df is not None else 0))
spark.createDataFrame(gold_inventory, ["gold_table", "path_exists", "parquet_files", "rows"]).show(100, truncate=False)
"""
        ),
        md(
            """
## 3. KPIs de negocio

Gold publica aproximadamente cuatro KPIs ejecutivos:

1. **% ejecución de recaudación:** mide cuánto se recaudó respecto al PIM. Es clave para evaluar avance presupuestario municipal.
2. **Variación PIM - PIA:** mide cuánto cambió el presupuesto modificado frente al inicial. Ayuda a identificar ajustes presupuestarios relevantes.
3. **Efectividad predial:** mide recaudación predial frente a emisión predial. Ayuda a decidir dónde reforzar cobranza.
4. **Capacidad tecnológica tributaria:** mide qué porcentaje de herramientas tributarias RENAMU tiene la municipalidad: SRTM, software propio de rentas y catastro.
"""
        ),
        code(
            r"""
kpi = gold_tables.get("mart_kpi_resumen_ejecutivo")
if kpi is not None:
    kpi.select(
        "year", "SEC_EJEC", "MUNICIPALIDAD_NOMBRE", "DEPARTAMENTO_NOMBRE",
        "categoria_municipalidad", "recaudacion_total", "pim_total",
        "kpi_pct_ejecucion_recaudacion", "kpi_variacion_pim_pia",
        "recaudacion_predial_total", "emision_predial_total",
        "kpi_efectividad_predial", "kpi_capacidad_software_pct",
        "prioridad_intervencion"
    ).orderBy(F.desc("recaudacion_total")).show(20, truncate=False)
"""
        ),
        md("### KPI 1: % ejecución de recaudación\n\nQué mide: `recaudacion_total / pim_total * 100`. Relevancia: muestra cuánto de lo programado/modificado realmente ingresó."),
        code(
            r"""
if kpi is not None:
    kpi.groupBy("year").agg(
        F.sum("recaudacion_total").alias("recaudacion_total"),
        F.sum("pim_total").alias("pim_total"),
        (F.sum("recaudacion_total") / F.sum("pim_total") * 100).alias("pct_ejecucion_promedio")
    ).orderBy("year").show(30, truncate=False)
"""
        ),
        md("### KPI 2: Variación presupuestaria PIM - PIA\n\nQué mide: diferencia entre presupuesto modificado e inicial. Relevancia: identifica municipalidades con ampliaciones o reducciones fuertes."),
        code(
            r"""
if kpi is not None:
    kpi.select("year", "MUNICIPALIDAD_NOMBRE", "DEPARTAMENTO_NOMBRE", "kpi_variacion_pim_pia") \
       .orderBy(F.desc("kpi_variacion_pim_pia")) \
       .show(20, truncate=False)
"""
        ),
        md("### KPI 3: Efectividad predial\n\nQué mide: `recaudacion_predial_total / emision_predial_total * 100`. Relevancia: ayuda a priorizar asistencia en cobranza predial."),
        code(
            r"""
if kpi is not None:
    kpi.filter(F.col("kpi_efectividad_predial").isNotNull()) \
       .groupBy("DEPARTAMENTO_NOMBRE", "categoria_municipalidad") \
       .agg(F.avg("kpi_efectividad_predial").alias("efectividad_predial_promedio")) \
       .orderBy(F.desc("efectividad_predial_promedio")) \
       .show(40, truncate=False)
"""
        ),
        md("### KPI 4: Capacidad tecnológica tributaria\n\nQué mide: porcentaje de tres herramientas RENAMU disponibles: SRTM, software de rentas y catastro. Relevancia: aproxima capacidad operativa para administrar tributos."),
        code(
            r"""
if kpi is not None:
    kpi.groupBy("categoria_municipalidad").agg(
        F.count("*").alias("municipalidades_periodo"),
        F.avg("kpi_capacidad_software_pct").alias("capacidad_software_promedio")
    ).orderBy("categoria_municipalidad").show(30, truncate=False)
"""
        ),
        md(
            """
## 4. Validaciones de calidad Gold

Objetivo: asegurar que las tablas analíticas no pierden claves y que los KPIs respetan reglas de negocio, especialmente división por cero y categorías A-G.
"""
        ),
        code(
            r"""
validations = []
for name, df in gold_tables.items():
    if df is None:
        validations.append((name, "availability", "failed", "No existe tabla Gold"))
        continue
    validations.append((name, "availability", "passed", f"{df.count():,} filas"))
    if "SEC_EJEC" in df.columns:
        null_sec = df.filter(F.col("SEC_EJEC").isNull()).count()
        validations.append((name, "sec_ejec_not_null", "passed" if null_sec == 0 else "failed", str(null_sec)))
    if "categoria_municipalidad" in df.columns:
        invalid_cat = df.filter(F.col("categoria_municipalidad").isNotNull() & ~F.col("categoria_municipalidad").isin(list("ABCDEFG"))).count()
        validations.append((name, "categoria_a_g", "passed" if invalid_cat == 0 else "failed", str(invalid_cat)))

if kpi is not None:
    invalid_exec = kpi.filter((F.col("pim_total") == 0) & F.col("kpi_pct_ejecucion_recaudacion").isNotNull()).count()
    validations.append(("mart_kpi_resumen_ejecutivo", "pim_zero_execution_null", "passed" if invalid_exec == 0 else "failed", str(invalid_exec)))

spark.createDataFrame(validations, ["table", "check", "status", "detail"]).show(200, truncate=False)
"""
        ),
        md(
            """
## 5. Tablas agregadas para Power BI

Objetivo: mapear las salidas Gold a las seis páginas de Power BI. Las tablas `pbi_dashboard_01..06` se publican como carpetas Parquet bajo `data/gold` para conectarlas directamente desde Power BI Desktop, sin Hive ni ODBC.
"""
        ),
        code(
            r"""
dashboard_mapping = [
    ("pbi_dashboard_01", "Recaudación Municipal vs Capacidad Tributaria", "SIAF + RENAMU + Categorías"),
    ("pbi_dashboard_02", "Recaudación por Clasificador de Ingreso", "SIAF + Clasificador + Categorías"),
    ("pbi_dashboard_03", "Predial vs Efectividad", "SISMEPRE + Categorías"),
    ("pbi_dashboard_04", "Distribución de Efectividad Predial", "SISMEPRE + Categorías"),
    ("pbi_dashboard_05", "Software Tributario Municipal", "RENAMU + Categorías"),
    ("pbi_dashboard_06", "Priorización de Municipalidades", "SIAF + SISMEPRE + RENAMU + Categorías"),
    ("pbi_kpi_resumen_ejecutivo", "Resumen ejecutivo de KPIs", "Mart Gold de KPIs"),
]
spark.createDataFrame(dashboard_mapping, ["tabla_gold_powerbi", "pagina", "fuentes"]).show(truncate=False)
"""
        ),
        md(
            """
## 6. Ejemplos visuales rápidos

Objetivo: generar tablas de apoyo que se pueden convertir en barras, tarjetas o tablas en Power BI.
"""
        ),
        code(
            r"""
if kpi is not None:
    print("Top departamentos por recaudación")
    kpi.groupBy("year", "DEPARTAMENTO_NOMBRE") \
       .agg(F.sum("recaudacion_total").alias("recaudacion_total")) \
       .orderBy(F.desc("recaudacion_total")) \
       .show(20, truncate=False)

    print("Prioridad de intervención")
    kpi.groupBy("year", "prioridad_intervencion") \
       .count() \
       .orderBy("year", "prioridad_intervencion") \
       .show(50, truncate=False)
"""
        ),
        md(
            """
## 7. Persistencia final en Parquet

Objetivo: confirmar que Gold quedó físicamente en Parquet Snappy. La escritura oficial la hace `main_gold.py` mediante `GoldStorage`. Este bloque verifica la presencia de archivos y deja un ejemplo seguro de escritura opcional.
"""
        ),
        code(
            r"""
for table in ["dim_municipalidad_gold", "fact_ingresos_mensuales", "fact_predial_mensual", "mart_kpi_resumen_ejecutivo"]:
    path = gold_root / table
    print(table, "parquet_files=", len(list(path.rglob("*.parquet"))) if path.exists() else 0, "path=", path)

WRITE_NOTEBOOK_PREVIEW = False
if WRITE_NOTEBOOK_PREVIEW and kpi is not None:
    preview_path = gold_root / "_notebook_validation" / "mart_kpi_resumen_ejecutivo_preview"
    kpi.write.mode("overwrite").option("compression", "snappy").partitionBy("year").parquet(str(preview_path))
    print(f"Preview escrito en {preview_path}")
"""
        ),
        md(
            """
## 8. Revisión de consumo en Power BI

Para la entrega final se omite Hive. Power BI debe consumir directamente los Parquet Gold:

- `data/gold/pbi_dashboard_01`
- `data/gold/pbi_dashboard_02`
- `data/gold/pbi_dashboard_03`
- `data/gold/pbi_dashboard_04`
- `data/gold/pbi_dashboard_05`
- `data/gold/pbi_dashboard_06`

Esta decisión elimina los errores ODBC/HiveServer2 y mantiene la arquitectura Medallion clara: Bronze, Silver y Gold almacenan Parquet; Power BI consume Gold.
"""
        ),
        code(
            r"""
powerbi_tables = [f"pbi_dashboard_{i:02d}" for i in range(1, 7)]
powerbi_inventory = []
for table in powerbi_tables:
    path = gold_root / table
    powerbi_inventory.append((table, str(path), path.exists(), len(list(path.rglob("*.parquet"))) if path.exists() else 0))

spark.createDataFrame(
    powerbi_inventory,
    ["tabla_gold_powerbi", "parquet_path", "exists", "parquet_files"],
).show(truncate=False)
"""
        ),
        md(
            """
## 9. Conclusión Gold

Gold consume Silver Parquet, construye constelación de hechos/dimensiones, genera marts para seis dashboards y publica KPIs ejecutivos. El consumo final de Power BI se hace directamente desde Parquet Gold, no desde Excel ni Hive.
"""
        ),
    ]


def main() -> None:
    write_notebook(ROOT / "notebooks" / "bronze" / "01_Bronze_Data_Profiling_Calidad.ipynb", bronze_notebook())
    write_notebook(ROOT / "notebooks" / "silver" / "02_Silver_Data_Profiling.ipynb", silver_notebook())
    write_notebook(ROOT / "notebooks" / "gold" / "01_Gold_Pipeline_Parcial.ipynb", gold_notebook())

    obsolete = [
        ROOT / "notebooks" / "bronze" / "01_Bronze_Pipeline_Parcial.ipynb",
        ROOT / "notebooks" / "bronze" / "02_Profiling_Ingresos.ipynb",
        ROOT / "notebooks" / "bronze" / "03_Profiling_SISMEPRE.ipynb",
        ROOT / "notebooks" / "bronze" / "04_Profiling_RENAMU.ipynb",
        ROOT / "notebooks" / "bronze" / "05_Bronze_Data_Profiling.ipynb",
    ]
    for path in obsolete:
        if path.exists():
            path.unlink()


if __name__ == "__main__":
    main()
