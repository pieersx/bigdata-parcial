from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession


GOLD_TABLES = [
    "dim_municipalidad_gold",
    "dim_ubigeo",
    "dim_tiempo",
    "dim_clasificador_ingreso",
    "dim_estado_sismepre",
    "dim_formulario_sismepre",
    "dim_pregunta_sismepre",
    "fact_ingresos_mensuales",
    "fact_ingresos_clasificador",
    "fact_predial_mensual",
    "fact_sismepre_cumplimiento",
    "fact_sismepre_respuestas_resumen",
    "fact_renamu_gestion_tributaria",
    "fact_renamu_software_at",
    "mart_dashboard_municipal",
    "fact_calidad_datos",
]

SILVER_TABLES = [
    "municipalidades_curated",
    "ingresos_municipales_curated",
    "predial_esat_curated",
    "sismepre_respuestas_curated",
    "sismepre_entidad_estado_curated",
    "sismepre_preguntas_curated",
    "sismepre_formularios_curated",
    "categorias_municipalidades_curated",
]


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("municipal-hive-bootstrap")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    metastore_uri = os.getenv("HIVE_METASTORE_URI")
    if metastore_uri:
        builder = builder.config("hive.metastore.uris", metastore_uri)
    return builder.enableHiveSupport().getOrCreate()


def execute_sql_file(spark: SparkSession, path: Path) -> None:
    if not path.exists():
        return
    statements = [stmt.strip() for stmt in path.read_text(encoding="utf-8").split(";")]
    for statement in statements:
        if statement and not statement.startswith("--"):
            spark.sql(statement)


def publish_table(
    spark: SparkSession,
    local_root: Path,
    hdfs_root: str,
    database: str,
    layer: str,
    table: str,
    mode: str,
    skip_copy: bool,
) -> dict:
    local_path = local_root / layer / table
    if not local_path.exists():
        return {"table": table, "status": "missing_local_path", "rows": 0}

    df = spark.read.option("basePath", str(local_path)).parquet(str(local_path))
    rows = df.count()
    external_location = f"{hdfs_root.rstrip('/')}/{layer}/{table}"

    if not skip_copy:
        (
            df.write.mode(mode)
            .option("compression", "snappy")
            .parquet(external_location)
        )
    else:
        external_location = str(local_path).replace("\\", "/")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
    spark.sql(
        f"""
        CREATE TABLE {database}.{table}
        USING PARQUET
        LOCATION '{external_location}'
        """
    )

    return {"table": table, "status": "published", "rows": rows, "location": external_location}


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish Medallion Parquet tables as Hive external tables.")
    parser.add_argument("--layer", choices=["gold", "silver", "all"], default="gold")
    parser.add_argument("--local-root", default="/home/jovyan/work/data")
    parser.add_argument("--hdfs-root", default=os.getenv("HDFS_ROOT", "hdfs://namenode:8020/datalake"))
    parser.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")
    parser.add_argument("--skip-copy", action="store_true", help="Register local Parquet paths without copying to HDFS.")
    args = parser.parse_args()

    spark = build_spark()
    local_root = Path(args.local_root)
    results: list[dict] = []

    if args.layer in {"silver", "all"}:
        for table in SILVER_TABLES:
            results.append(
                publish_table(
                    spark,
                    local_root,
                    args.hdfs_root,
                    "municipal_silver",
                    "silver",
                    table,
                    args.mode,
                    args.skip_copy,
                )
            )

    if args.layer in {"gold", "all"}:
        for table in GOLD_TABLES:
            results.append(
                publish_table(
                    spark,
                    local_root,
                    args.hdfs_root,
                    "municipal_gold",
                    "gold",
                    table,
                    args.mode,
                    args.skip_copy,
                )
            )
        execute_sql_file(spark, Path("/home/jovyan/work/sql/hive/03_dashboard_views.sql"))

    print("Hive bootstrap summary")
    for item in results:
        print(item)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
