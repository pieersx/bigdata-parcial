from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession


EXPORT_QUERIES = {
    "01_recaudacion_capacidad": """
        SELECT *
        FROM municipal_gold.vw_dashboard_01_recaudacion_capacidad
        ORDER BY year DESC, recaudacion_total DESC
        LIMIT {row_limit}
    """,
    "02_clasificador_ingreso": """
        SELECT
            f.year,
            m.DEPARTAMENTO_NOMBRE,
            m.PROVINCIA_NOMBRE,
            m.DISTRITO_NOMBRE,
            m.categoria_municipalidad,
            c.RUBRO_NOMBRE,
            c.ESPECIFICA_NOMBRE,
            SUM(f.MONTO_PIA) AS pia,
            SUM(f.MONTO_PIM) AS pim,
            SUM(f.MONTO_RECAUDADO) AS recaudado
        FROM municipal_gold.fact_ingresos_clasificador f
        JOIN municipal_gold.dim_municipalidad_gold m ON f.SEC_EJEC = m.SEC_EJEC
        LEFT JOIN municipal_gold.dim_clasificador_ingreso c ON f.clasificador_id = c.clasificador_id
        WHERE f.year >= 2022
        GROUP BY
            f.year,
            m.DEPARTAMENTO_NOMBRE,
            m.PROVINCIA_NOMBRE,
            m.DISTRITO_NOMBRE,
            m.categoria_municipalidad,
            c.RUBRO_NOMBRE,
            c.ESPECIFICA_NOMBRE
        ORDER BY recaudado DESC
        LIMIT {row_limit}
    """,
    "03_predial_vs_efectividad": """
        SELECT *
        FROM municipal_gold.vw_dashboard_03_predial_vs_efectividad
        ORDER BY year DESC, recaudacion_predial_total DESC
        LIMIT {row_limit}
    """,
    "04_distribucion_efectividad": """
        SELECT *
        FROM municipal_gold.vw_dashboard_04_distribucion_efectividad
        ORDER BY year DESC, efectividad_predial_pct DESC
        LIMIT {row_limit}
    """,
    "05_software_tributario": """
        SELECT *
        FROM municipal_gold.vw_dashboard_05_software_tributario
        LIMIT {row_limit}
    """,
    "06_priorizacion_municipal": """
        SELECT *
        FROM municipal_gold.vw_dashboard_06_priorizacion_municipal
        ORDER BY year DESC, prioridad_intervencion, saldo_predial_total DESC
        LIMIT {row_limit}
    """,
}


def build_spark() -> SparkSession:
    builder = SparkSession.builder.appName("municipal-hive-powerbi-export")
    metastore_uri = os.getenv("HIVE_METASTORE_URI")
    if metastore_uri:
        builder = builder.config("hive.metastore.uris", metastore_uri)
    return builder.enableHiveSupport().getOrCreate()


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Hive dashboard views to a Power BI-ready Excel workbook.")
    parser.add_argument("--output", default="/home/jovyan/work/data/powerbi/powerbi_municipal_hive.xlsx")
    parser.add_argument("--row-limit", type=int, default=25000)
    args = parser.parse_args()

    spark = build_spark()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    with __import__("pandas").ExcelWriter(output, engine="openpyxl") as writer:
        for sheet, query in EXPORT_QUERIES.items():
            df = spark.sql(query.format(row_limit=args.row_limit))
            pdf = df.toPandas()
            pdf.to_excel(writer, sheet_name=sheet[:31], index=False)

        dax = __import__("pandas").DataFrame(
            [
                ("Recaudacion Total", "SUM('01_recaudacion_capacidad'[recaudacion_total])"),
                ("PIM Total", "SUM('02_clasificador_ingreso'[pim])"),
                ("Pct Ejecucion", "DIVIDE([Recaudacion Total], [PIM Total])"),
                ("Personal Municipal", "SUM('01_recaudacion_capacidad'[personal_municipal_total])"),
                ("Recaudacion Por Personal", "DIVIDE([Recaudacion Total], [Personal Municipal])"),
                ("Recaudacion Predial", "SUM('03_predial_vs_efectividad'[recaudacion_predial_total])"),
                ("Emision Predial", "SUM('03_predial_vs_efectividad'[emision_predial_total])"),
                ("Efectividad Predial", "DIVIDE([Recaudacion Predial], [Emision Predial])"),
                ("Municipalidades Con Software AT", "COUNTROWS(FILTER('05_software_tributario', '05_software_tributario'[usa_al_menos_un_software_at] = TRUE()))"),
            ],
            columns=["Medida", "DAX"],
        )
        dax.to_excel(writer, sheet_name="medidas_dax", index=False)

    print(f"Power BI Hive workbook written to {output}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
