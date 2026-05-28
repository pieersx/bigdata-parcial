from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.quality.data_quality_analyzer import DataQualityAnalyzer
from app.profiling.profiling_generator import ProfilingGenerator
from app.spark.readers import read_csv_as_bronze_dataframe
from app.storage.parquet_storage import ParquetStorage
from app.utils.logger import StructuredLogger


class BronzeTransformService:
    def __init__(
        self,
        spark: SparkSession,
        parquet_storage: ParquetStorage,
        quality_analyzer: Optional[DataQualityAnalyzer] = None,
        profiling_generator: Optional[ProfilingGenerator] = None,
    ):
        self.spark = spark
        self.parquet_storage = parquet_storage
        self.quality_analyzer = quality_analyzer
        self.profiling_generator = profiling_generator
        self.logger = StructuredLogger(self.__class__.__name__)

    def transform(self, dataset: str, asset: Dict[str, Any], landing_result: Dict[str, Any]) -> Dict[str, Any]:
        raw_path = Path(landing_result["raw_path"])
        df = read_csv_as_bronze_dataframe(self.spark, raw_path, asset["read_options"])
        df = self._add_bronze_metadata(df, dataset, asset, raw_path)

        partition_columns: List[str] = []
        year_column = asset.get("year_column")
        if year_column:
            df = self._add_year_partition(df, year_column, asset.get("default_year"))
            partition_columns = ["year"]

        df = df.cache()
        records_count = df.count()

        storage_result = self.parquet_storage.write(
            df=df,
            dataset=dataset,
            table_name=asset["table_name"],
            asset_role=asset["asset_role"],
            partition_columns=partition_columns,
        )

        quality_results = []
        if self.quality_analyzer and asset.get("quality_enabled", False):
            quality_results = self.quality_analyzer.analyze(
                df=df,
                dataset=dataset,
                table_name=asset["table_name"],
                asset=asset,
                bronze_path=Path(storage_result["bronze_path"]),
            )

        profiling_results = []
        if self.profiling_generator and asset.get("profiling_enabled", False):
            profiling_results = self.profiling_generator.generate(
                spark=self.spark,
                dataset=dataset,
                table_name=asset["table_name"],
                asset=asset,
                bronze_path=Path(storage_result["bronze_path"]),
            )

        df.unpersist()

        self.logger.info(
            "Bronze transformation completed",
            dataset=dataset,
            asset_name=asset["name"],
            table_name=asset["table_name"],
            records_count=records_count,
            bronze_path=storage_result["bronze_path"],
        )

        return {
            **landing_result,
            **storage_result,
            "status": "success",
            "records_count": records_count,
            "quality_checks": quality_results,
            "profiling_reports": profiling_results,
        }

    def _add_bronze_metadata(
        self,
        df: DataFrame,
        dataset: str,
        asset: Dict[str, Any],
        raw_path: Path,
    ) -> DataFrame:
        return (
            df.withColumn("_bronze_dataset", F.lit(dataset))
            .withColumn("_bronze_asset_name", F.lit(asset["name"]))
            .withColumn("_bronze_table_name", F.lit(asset["table_name"]))
            .withColumn("_bronze_asset_role", F.lit(asset["asset_role"]))
            .withColumn("_bronze_source_type", F.lit(asset["source_type"]))
            .withColumn("_bronze_source_path", F.lit(str(raw_path)))
            .withColumn("_bronze_ingestion_ts", F.current_timestamp())
        )

    def _add_year_partition(
        self,
        df: DataFrame,
        year_column: str,
        default_year: Optional[int] = None,
    ) -> DataFrame:
        year_value = F.regexp_extract(F.trim(F.col(year_column)), r"(\d{4})", 1)
        if default_year is not None:
            year_value = F.when(year_value != "", year_value).otherwise(F.lit(str(default_year)))
        return df.withColumn("year", year_value)
