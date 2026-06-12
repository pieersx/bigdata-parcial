from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession


POWERBI_TABLES = {
    "pbi_dashboard_01": """
        SELECT
            CAST(year AS INT) AS year,
            CAST(sec_ejec AS STRING) AS sec_ejec,
            CAST(ubigeo AS STRING) AS ubigeo,
            CAST(municipalidad_nombre AS STRING) AS municipalidad_nombre,
            CAST(departamento_nombre AS STRING) AS departamento_nombre,
            CAST(provincia_nombre AS STRING) AS provincia_nombre,
            CAST(distrito_nombre AS STRING) AS distrito_nombre,
            CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
            CAST(recaudacion_total AS DOUBLE) AS recaudacion_total,
            CAST(pim_total AS DOUBLE) AS pim_total,
            CAST(personal_municipal_total AS INT) AS personal_municipal_total,
            CAST(requiere_asistencia_at AS STRING) AS requiere_asistencia_at,
            CAST(recaudacion_por_personal AS DOUBLE) AS recaudacion_por_personal
        FROM municipal_gold.vw_dashboard_01_recaudacion_capacidad
    """,
    "pbi_dashboard_02": """
        SELECT
            CAST(year AS INT) AS year,
            CAST(month AS INT) AS month,
            CAST(sec_ejec AS STRING) AS sec_ejec,
            CAST(departamento_nombre AS STRING) AS departamento_nombre,
            CAST(provincia_nombre AS STRING) AS provincia_nombre,
            CAST(distrito_nombre AS STRING) AS distrito_nombre,
            CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
            CAST(clasificador_id AS STRING) AS clasificador_id,
            CAST(rubro_nombre AS STRING) AS rubro_nombre,
            CAST(generica_nombre AS STRING) AS generica_nombre,
            CAST(subgenerica_nombre AS STRING) AS subgenerica_nombre,
            CAST(especifica_nombre AS STRING) AS especifica_nombre,
            CAST(pia AS DOUBLE) AS pia,
            CAST(pim AS DOUBLE) AS pim,
            CAST(recaudado AS DOUBLE) AS recaudado
        FROM municipal_gold.vw_dashboard_02_clasificador_ingreso
    """,
    "pbi_dashboard_03": """
        SELECT
            CAST(year AS INT) AS year,
            CAST(month AS INT) AS month,
            CAST(sec_ejec AS STRING) AS sec_ejec,
            CAST(departamento_nombre AS STRING) AS departamento_nombre,
            CAST(provincia_nombre AS STRING) AS provincia_nombre,
            CAST(distrito_nombre AS STRING) AS distrito_nombre,
            CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
            CAST(recaudacion_predial_total AS DOUBLE) AS recaudacion_predial_total,
            CAST(emision_predial_total AS DOUBLE) AS emision_predial_total,
            CAST(efectividad_predial_pct AS DOUBLE) AS efectividad_predial_pct,
            CAST(estado_sismepre AS STRING) AS estado_sismepre,
            CAST(clasificacion_sismepre AS STRING) AS clasificacion_sismepre,
            CAST(tipo_meta AS STRING) AS tipo_meta
        FROM municipal_gold.vw_dashboard_03_predial_vs_efectividad
    """,
    "pbi_dashboard_04": """
        SELECT
            CAST(year AS INT) AS year,
            CAST(sec_ejec AS STRING) AS sec_ejec,
            CAST(departamento_nombre AS STRING) AS departamento_nombre,
            CAST(provincia_nombre AS STRING) AS provincia_nombre,
            CAST(distrito_nombre AS STRING) AS distrito_nombre,
            CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
            CAST(recaudacion_predial_total AS DOUBLE) AS recaudacion_predial_total,
            CAST(emision_predial_total AS DOUBLE) AS emision_predial_total,
            CAST(efectividad_predial_pct AS DOUBLE) AS efectividad_predial_pct,
            CAST(decil_efectividad AS INT) AS decil_efectividad,
            CAST(ranking_departamental AS INT) AS ranking_departamental
        FROM municipal_gold.vw_dashboard_04_distribucion_efectividad
    """,
    "pbi_dashboard_05": """
        SELECT
            CAST(year AS INT) AS year,
            CAST(sec_ejec AS STRING) AS sec_ejec,
            CAST(departamento_nombre AS STRING) AS departamento_nombre,
            CAST(provincia_nombre AS STRING) AS provincia_nombre,
            CAST(distrito_nombre AS STRING) AS distrito_nombre,
            CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
            CAST(usa_srtm_estado AS STRING) AS usa_srtm_estado,
            CAST(usa_software_rentas_at AS STRING) AS usa_software_rentas_at,
            CAST(usa_software_catastro AS STRING) AS usa_software_catastro,
            CAST(usa_al_menos_un_software_at AS STRING) AS usa_al_menos_un_software_at
        FROM municipal_gold.vw_dashboard_05_software_tributario
    """,
    "pbi_dashboard_06": """
        SELECT
            CAST(year AS INT) AS year,
            CAST(sec_ejec AS STRING) AS sec_ejec,
            CAST(ubigeo AS STRING) AS ubigeo,
            CAST(municipalidad_nombre AS STRING) AS municipalidad_nombre,
            CAST(departamento_nombre AS STRING) AS departamento_nombre,
            CAST(provincia_nombre AS STRING) AS provincia_nombre,
            CAST(distrito_nombre AS STRING) AS distrito_nombre,
            CAST(categoria_municipalidad AS STRING) AS categoria_municipalidad,
            CAST(recaudacion_total AS DOUBLE) AS recaudacion_total,
            CAST(recaudacion_predial_total AS DOUBLE) AS recaudacion_predial_total,
            CAST(base_imponible_predial AS DOUBLE) AS base_imponible_predial,
            CAST(saldo_predial_total AS DOUBLE) AS saldo_predial_total,
            CAST(efectividad_predial_pct AS DOUBLE) AS efectividad_predial_pct,
            CAST(usa_al_menos_un_software_at AS STRING) AS usa_al_menos_un_software_at,
            CAST(estado_sismepre AS STRING) AS estado_sismepre,
            CAST(clasificacion_sismepre AS STRING) AS clasificacion_sismepre,
            CAST(tipo_meta_sismepre AS STRING) AS tipo_meta_sismepre,
            CAST(prioridad_intervencion AS STRING) AS prioridad_intervencion
        FROM municipal_gold.vw_dashboard_06_priorizacion_municipal
    """,
}

SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "hive"
TEMP_DASHBOARD_VIEWS = [
    "vw_dashboard_01_recaudacion_capacidad",
    "vw_dashboard_02_clasificador_ingreso",
    "vw_dashboard_03_predial_vs_efectividad",
    "vw_dashboard_04_distribucion_efectividad",
    "vw_dashboard_05_software_tributario",
    "vw_dashboard_06_priorizacion_municipal",
]


def execute_sql_file(spark: SparkSession, path: Path) -> None:
    for statement in path.read_text(encoding="utf-8").split(";"):
        statement = statement.strip()
        if statement and not statement.startswith("--"):
            spark.sql(statement)


def drop_temp_views(spark: SparkSession) -> None:
    for view_name in TEMP_DASHBOARD_VIEWS:
        spark.sql(f"DROP VIEW IF EXISTS municipal_gold.{view_name}")


def main() -> int:
    builder = (
        SparkSession.builder.appName("materialize-powerbi-hive-tables")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "48"))
        .config("spark.sql.adaptive.enabled", "true")
    )
    metastore_uri = os.getenv("HIVE_METASTORE_URI")
    if metastore_uri:
        builder = builder.config("hive.metastore.uris", metastore_uri)
    spark = builder.enableHiveSupport().getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS municipal_gold")
    spark.sql("USE municipal_gold")
    execute_sql_file(spark, SQL_DIR / "03_dashboard_views.sql")

    for table_name, query in POWERBI_TABLES.items():
        full_name = f"municipal_gold.{table_name}"
        location = f"hdfs://namenode:8020/datalake/powerbi/{table_name}"
        print(f"Materializing {full_name}...")
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        df = spark.sql(query)
        (
            df.write.mode("overwrite")
            .format("parquet")
            .option("compression", "snappy")
            .save(location)
        )
        spark.sql(f"CREATE TABLE {full_name} USING PARQUET LOCATION '{location}'")
        print(f"{full_name}: {df.count()} rows")

    drop_temp_views(spark)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
