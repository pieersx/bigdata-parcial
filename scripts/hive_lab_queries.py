from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession


QUERIES = {
    "01_lectura_parquet": """
        SELECT COUNT(*) AS filas
        FROM municipal_gold.fact_ingresos_mensuales
    """,
    "02_agregacion_recaudacion": """
        SELECT
            year,
            SUM(MONTO_PIA) AS pia,
            SUM(MONTO_PIM) AS pim,
            SUM(MONTO_RECAUDADO) AS recaudado
        FROM municipal_gold.fact_ingresos_mensuales
        GROUP BY year
        ORDER BY year
    """,
    "03_filtrado_ordenamiento": """
        SELECT
            m.DEPARTAMENTO_NOMBRE,
            m.PROVINCIA_NOMBRE,
            m.DISTRITO_NOMBRE,
            SUM(i.MONTO_RECAUDADO) AS recaudado
        FROM municipal_gold.fact_ingresos_mensuales i
        JOIN municipal_gold.dim_municipalidad_gold m ON i.SEC_EJEC = m.SEC_EJEC
        WHERE i.year = 2024
        GROUP BY m.DEPARTAMENTO_NOMBRE, m.PROVINCIA_NOMBRE, m.DISTRITO_NOMBRE
        ORDER BY recaudado DESC
        LIMIT 20
    """,
    "04_datos_faltantes": """
        SELECT
            COALESCE(categoria_municipalidad, 'SIN CATEGORIA') AS categoria,
            COUNT(*) AS municipalidades
        FROM municipal_gold.dim_municipalidad_gold
        GROUP BY COALESCE(categoria_municipalidad, 'SIN CATEGORIA')
        ORDER BY categoria
    """,
    "05_window_ranking": """
        WITH ranking AS (
            SELECT
                m.DEPARTAMENTO_NOMBRE,
                m.DISTRITO_NOMBRE,
                SUM(i.MONTO_RECAUDADO) AS recaudado,
                RANK() OVER (
                    PARTITION BY m.DEPARTAMENTO_NOMBRE
                    ORDER BY SUM(i.MONTO_RECAUDADO) DESC
                ) AS ranking_departamento
            FROM municipal_gold.fact_ingresos_mensuales i
            JOIN municipal_gold.dim_municipalidad_gold m ON i.SEC_EJEC = m.SEC_EJEC
            WHERE i.year = 2024
            GROUP BY m.DEPARTAMENTO_NOMBRE, m.DISTRITO_NOMBRE
        )
        SELECT *
        FROM ranking
        WHERE ranking_departamento <= 5
        ORDER BY DEPARTAMENTO_NOMBRE, ranking_departamento
    """,
    "06_cte_join_analytics": """
        WITH ingresos AS (
            SELECT SEC_EJEC, year, SUM(MONTO_RECAUDADO) AS recaudado
            FROM municipal_gold.fact_ingresos_mensuales
            GROUP BY SEC_EJEC, year
        ),
        predial AS (
            SELECT
                SEC_EJEC,
                year,
                SUM(MON_RECAUDACION_TOTAL) AS reca_predial,
                SUM(MON_EMISIONPREDIAL_AFECTO) AS emision_predial
            FROM municipal_gold.fact_predial_mensual
            GROUP BY SEC_EJEC, year
        )
        SELECT
            m.DEPARTAMENTO_NOMBRE,
            m.categoria_municipalidad,
            i.year,
            SUM(i.recaudado) AS recaudado_siaf,
            SUM(p.reca_predial) AS recaudado_predial,
            CASE
                WHEN SUM(p.emision_predial) = 0 THEN NULL
                ELSE SUM(p.reca_predial) / SUM(p.emision_predial) * 100
            END AS efectividad_predial_pct
        FROM ingresos i
        JOIN municipal_gold.dim_municipalidad_gold m ON i.SEC_EJEC = m.SEC_EJEC
        LEFT JOIN predial p ON i.SEC_EJEC = p.SEC_EJEC AND i.year = p.year
        GROUP BY m.DEPARTAMENTO_NOMBRE, m.categoria_municipalidad, i.year
        ORDER BY i.year DESC, recaudado_siaf DESC
    """,
}


def build_spark() -> SparkSession:
    builder = SparkSession.builder.appName("municipal-hive-lab")
    metastore_uri = os.getenv("HIVE_METASTORE_URI")
    if metastore_uri:
        builder = builder.config("hive.metastore.uris", metastore_uri)
    return builder.enableHiveSupport().getOrCreate()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Hive lab queries for the municipal BigData project.")
    parser.add_argument("--output-root", default="/home/jovyan/work/data/hive/results")
    parser.add_argument("--report-path", default="/home/jovyan/work/reports/hive_lab_results.md")
    parser.add_argument("--create-views", action="store_true")
    args = parser.parse_args()

    spark = build_spark()
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_lines = ["# Resultados Laboratorio Hive Municipal", ""]

    if args.create_views:
        sql_path = Path("/home/jovyan/work/sql/hive/03_dashboard_views.sql")
        for statement in sql_path.read_text(encoding="utf-8").split(";"):
            statement = statement.strip()
            if statement and not statement.startswith("--"):
                spark.sql(statement)

    for name, query in QUERIES.items():
        df = spark.sql(query)
        count = df.count()
        target = output_root / name
        df.write.mode("overwrite").option("compression", "snappy").parquet(str(target))
        preview = df.limit(5).toPandas()
        report_lines.append(f"## {name}")
        report_lines.append("")
        report_lines.append(f"- Filas resultado: `{count}`")
        report_lines.append(f"- Parquet: `{target.as_posix()}`")
        report_lines.append("")
        report_lines.append("```text")
        report_lines.append(preview.to_string(index=False))
        report_lines.append("```")
        report_lines.append("")

    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"Hive lab report written to {report_path}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
