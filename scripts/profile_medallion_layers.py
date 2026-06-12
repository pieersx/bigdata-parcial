from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ROOT = Path("/home/jovyan/work")
LAYERS = {
    "bronze": ROOT / "data" / "bronze",
    "silver": ROOT / "data" / "silver",
    "gold": ROOT / "data" / "gold",
}
REPORTS = ROOT / "reports" / "profiling"
SAMPLE_ROWS = 8


def parquet_tables(layer_path: Path):
    tables = []
    for path in sorted(layer_path.iterdir()):
        if not path.is_dir() or path.name == "_quarantine":
            continue
        if any(path.glob("*.parquet")):
            tables.append(path)
            continue
        children = [child for child in path.iterdir() if child.is_dir() and not child.name.startswith("_")]
        if children and all("=" in child.name for child in children) and any(path.rglob("*.parquet")):
            tables.append(path)
            continue
        for child in sorted(children):
            if child.name == "_quarantine" or child.name.startswith("_"):
                continue
            if any(child.rglob("*.parquet")):
                tables.append(child)
    return tables


def html_table(headers, rows):
    head = "".join(f"<th>{header}</th>" for header in headers)
    body = "\n".join(
        "<tr>" + "".join(f"<td>{'' if value is None else value}</td>" for value in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def profile_table(spark: SparkSession, layer: str, table_path: Path):
    table_name = "__".join(table_path.relative_to(LAYERS[layer]).parts)
    df = spark.read.option("basePath", str(table_path)).parquet(str(table_path))
    total = df.count()
    null_counts = {}
    if df.columns:
        null_counts = df.select(
            *[
                F.sum(F.when(F.col(column).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(column)
                for column in df.columns
            ]
        ).first().asDict()
    schema_rows = []
    for field in df.schema.fields:
        column = field.name
        nulls = int(null_counts.get(column) or 0)
        null_pct = round(nulls / total * 100, 4) if total else 0
        schema_rows.append((column, field.dataType.simpleString(), nulls, null_pct))

    sample = df.limit(SAMPLE_ROWS).toPandas()
    sample_rows = sample.astype(str).values.tolist() if not sample.empty else []
    output_dir = REPORTS / layer / table_name
    output_dir.mkdir(parents=True, exist_ok=True)
    html = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <title>Profiling {layer} - {table_name}</title>
  <style>
    body {{ font-family: Segoe UI, Arial, sans-serif; margin: 32px; color: #222; }}
    h1 {{ color: #16892d; }}
    table {{ border-collapse: collapse; width: 100%; margin: 16px 0 28px; font-size: 13px; }}
    th {{ background: #18a837; color: white; text-align: left; padding: 8px; }}
    td {{ border: 1px solid #ddd; padding: 6px; vertical-align: top; }}
    .kpi {{ display: inline-block; margin-right: 24px; padding: 12px 18px; background: #e2ffd8; }}
  </style>
</head>
<body>
  <h1>Profiling {layer.title()} - {table_name}</h1>
  <div class="kpi"><strong>Registros:</strong> {total:,}</div>
  <div class="kpi"><strong>Columnas:</strong> {len(df.columns)}</div>
  <h2>Esquema y nulos</h2>
  {html_table(["Columna", "Tipo", "Nulos", "% Nulos"], schema_rows)}
  <h2>Muestra</h2>
  {html_table(df.columns, sample_rows)}
</body>
</html>
"""
    (output_dir / "index.html").write_text(html, encoding="utf-8")
    return table_name, total, len(df.columns)


def write_layer_index(layer: str, rows):
    output_dir = REPORTS / layer
    output_dir.mkdir(parents=True, exist_ok=True)
    links = "\n".join(
        f'<tr><td><a href="{table}/index.html">{table}</a></td><td>{records:,}</td><td>{columns}</td></tr>'
        for table, records, columns in rows
    )
    html = f"""<!doctype html>
<html lang="es"><head><meta charset="utf-8" /><title>Profiling {layer}</title>
<style>body{{font-family:Segoe UI,Arial,sans-serif;margin:32px}}th{{background:#18a837;color:#fff}}td,th{{padding:8px;border:1px solid #ddd}}table{{border-collapse:collapse}}</style>
</head><body><h1>Profiling {layer.title()}</h1><table><tr><th>Tabla</th><th>Registros</th><th>Columnas</th></tr>{links}</table></body></html>"""
    (output_dir / "index.html").write_text(html, encoding="utf-8")


def main():
    spark = SparkSession.builder.appName("profile-medallion-layers").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for layer, path in LAYERS.items():
        rows = [profile_table(spark, layer, table) for table in parquet_tables(path)]
        write_layer_index(layer, rows)
        print(f"{layer}: {len(rows)} profiles")
    spark.stop()


if __name__ == "__main__":
    main()
