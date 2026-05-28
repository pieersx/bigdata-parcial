from pathlib import Path
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession


def read_csv_as_bronze_dataframe(
    spark: SparkSession,
    file_path: Path,
    read_options: Dict[str, Any],
) -> DataFrame:
    encoding = read_options.get("encoding", "utf-8")
    if encoding.lower() == "utf-8-sig":
        encoding = "UTF-8"

    options = {
        "header": str(read_options.get("header", True)).lower(),
        "sep": read_options.get("delimiter", ","),
        "encoding": encoding,
        "mode": read_options.get("mode", "PERMISSIVE"),
        "inferSchema": "false",
        "multiLine": str(read_options.get("multiLine", False)).lower(),
    }
    return spark.read.options(**options).csv(str(file_path))
