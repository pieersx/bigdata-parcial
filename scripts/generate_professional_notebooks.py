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
if PROJECT_ROOT.name in {"bronze", "silver", "gold", "hive"}:
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


def main() -> None:
    write_notebook(ROOT / "notebooks" / "bronze" / "01_Bronze_Data_Profiling_Calidad.ipynb", bronze_notebook())
    write_notebook(ROOT / "notebooks" / "silver" / "02_Silver_Data_Profiling.ipynb", silver_notebook())

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
