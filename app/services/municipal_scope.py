import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType


SCOPE_FILENAME = "municipalidades_presentadas.csv"
SCOPE_COLUMNS = ["SEC_EJEC", "UBIGEO", "MUNICIPALIDAD_NOMBRE"]


@dataclass
class ScopeResult:
    dataframe: DataFrame
    metrics: Dict[str, Any]


class MunicipalScopeService:
    def __init__(self, spark: SparkSession, reference_path: str | Path):
        self.spark = spark
        self.reference_path = Path(reference_path)
        self.scope_file = self.reference_path / SCOPE_FILENAME

    def apply_to_master(self, municipalities: DataFrame) -> ScopeResult:
        self._ensure_template()
        base_total = municipalities.count()
        scope = self._read_scope()

        if scope is None:
            return ScopeResult(
                dataframe=municipalities.withColumn("in_scope_presentacion", F.lit(True)),
                metrics={
                    "scope_status": "pendiente_archivo_profesor",
                    "scope_file": str(self.scope_file),
                    "total_siaf_municipal": base_total,
                    "selected_count": base_total,
                    "excluded_count": 0,
                    "not_found_count": 0,
                    "duplicate_count": 0,
                    "match_key": "all",
                },
            )

        duplicate_count = scope.count() - scope.dropDuplicates(["scope_key"]).count()
        scope_unique = scope.dropDuplicates(["scope_key"])
        matched = (
            municipalities.join(scope_unique.select("scope_key"), municipalities.SEC_EJEC == scope_unique.scope_key, "left")
            .withColumn("matched_by_sec_ejec", F.col("scope_key").isNotNull())
            .drop("scope_key")
        )
        matched = (
            matched.join(scope_unique.select("scope_key"), matched.UBIGEO == scope_unique.scope_key, "left")
            .withColumn("matched_by_ubigeo", F.col("scope_key").isNotNull())
            .drop("scope_key")
        )
        curated = matched.withColumn(
            "in_scope_presentacion",
            F.col("matched_by_sec_ejec") | F.col("matched_by_ubigeo"),
        ).drop("matched_by_sec_ejec", "matched_by_ubigeo")

        selected_count = curated.filter("in_scope_presentacion").count()
        matched_keys = (
            municipalities.select(F.col("SEC_EJEC").alias("scope_key"))
            .unionByName(municipalities.select(F.col("UBIGEO").alias("scope_key")))
            .dropDuplicates(["scope_key"])
        )
        not_found_count = scope_unique.join(matched_keys, "scope_key", "left_anti").count()

        return ScopeResult(
            dataframe=curated,
            metrics={
                "scope_status": "aplicado",
                "scope_file": str(self.scope_file),
                "total_siaf_municipal": base_total,
                "scope_input_count": scope.count(),
                "selected_count": selected_count,
                "excluded_count": base_total - selected_count,
                "not_found_count": not_found_count,
                "duplicate_count": duplicate_count,
                "match_key": "SEC_EJEC_or_UBIGEO",
            },
        )

    def _ensure_template(self) -> None:
        self.reference_path.mkdir(parents=True, exist_ok=True)
        if self.scope_file.exists():
            return
        with open(self.scope_file, "w", newline="", encoding="utf-8") as file_handle:
            writer = csv.writer(file_handle)
            writer.writerow(SCOPE_COLUMNS)

    def _read_scope(self) -> DataFrame | None:
        if not self.scope_file.exists() or self.scope_file.stat().st_size == 0:
            return None

        schema = StructType([StructField(column, StringType(), True) for column in SCOPE_COLUMNS])
        scope = self.spark.read.option("header", True).schema(schema).csv(str(self.scope_file))
        available = [column for column in SCOPE_COLUMNS if column in scope.columns]
        if not available:
            return None

        normalized = (
            scope.select(
                F.when(F.trim(F.col("SEC_EJEC")) != "", F.trim(F.col("SEC_EJEC"))).alias("SEC_EJEC"),
                F.regexp_replace(F.trim(F.col("UBIGEO")), r"\.0$", "").alias("raw_ubigeo"),
                F.upper(F.trim(F.col("MUNICIPALIDAD_NOMBRE"))).alias("MUNICIPALIDAD_NOMBRE"),
            )
            .withColumn(
                "UBIGEO",
                F.when(F.col("raw_ubigeo").rlike(r"^97[0-9]{3}$"), F.col("raw_ubigeo"))
                .when(F.col("raw_ubigeo") != "", F.lpad("raw_ubigeo", 6, "0")),
            )
            .drop("raw_ubigeo")
            .withColumn("scope_key", F.coalesce(F.col("SEC_EJEC"), F.col("UBIGEO")))
            .filter(F.col("scope_key").isNotNull() & (F.col("scope_key") != ""))
        )
        if normalized.limit(1).count() == 0:
            return None
        return normalized
