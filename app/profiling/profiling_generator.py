from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

from app.storage.data_lake import DataLake
from app.utils.logger import StructuredLogger


class ProfilingGenerator:
    def __init__(self, data_lake: DataLake, profiling_config: Optional[Dict[str, Any]] = None):
        self.data_lake = data_lake
        self.profiling_config = profiling_config or {}
        self.sample_limit = int(self.profiling_config.get("sample_limit", 50000))
        self.minimal = bool(self.profiling_config.get("minimal", True))
        self.logger = StructuredLogger(self.__class__.__name__)

    def generate(
        self,
        spark: SparkSession,
        dataset: str,
        table_name: str,
        asset: Dict[str, Any],
        bronze_path: Path,
    ) -> List[str]:
        if not asset.get("year_column"):
            return []

        profile_dir = self.data_lake.resolve_reports_path(dataset, None if table_name == dataset else table_name)
        year_partitions = sorted(bronze_path.glob("year=*"))
        generated_files: List[str] = []

        for partition_dir in year_partitions:
            year_value = partition_dir.name.split("=", 1)[1]
            profile_path = profile_dir / f"profiling_{year_value}.html"
            self._generate_profile_file(spark, dataset, table_name, partition_dir, year_value, profile_path)
            generated_files.append(str(profile_path))

        self._write_table_index(profile_dir, dataset, table_name)
        self._write_dataset_index(dataset)

        self.logger.info(
            "Profiling reports generated",
            dataset=dataset,
            table_name=table_name,
            generated_files=generated_files,
        )
        return generated_files

    def _generate_profile_file(
        self,
        spark: SparkSession,
        dataset: str,
        table_name: str,
        partition_dir: Path,
        year_value: str,
        profile_path: Path,
    ):
        from ydata_profiling import ProfileReport

        df = spark.read.parquet(str(partition_dir))

        selected_columns = self._profile_columns(dataset, table_name, df.columns)
        pdf = df.select(*selected_columns).limit(self.sample_limit).toPandas()
        profile = ProfileReport(
            pdf,
            title=f"{dataset} - {table_name} - {year_value}",
            minimal=self.minimal,
        )
        profile.to_file(profile_path)

    def _profile_columns(self, dataset: str, table_name: str, available_columns: List[str]) -> List[str]:
        if dataset == "renamu":
            preferred_columns = [
                "Año",
                "idmunici",
                "ccdd",
                "ccpp",
                "ccdi",
                "Ubigeo",
                "Departamento",
                "Provincia",
                "Distrito",
                "Tipomuni",
            ]
            return [column for column in preferred_columns if column in available_columns]
        return [column for column in available_columns if not column.startswith("_bronze_")]

    def _write_table_index(self, profile_dir: Path, dataset: str, table_name: str):
        profile_files = sorted(profile_dir.glob("profiling_*.html"))
        links = "\n".join(
            f'<li><a href="{profile_file.name}">{profile_file.stem.replace("profiling_", "Año ")}</a></li>'
            for profile_file in profile_files
        )
        html = (
            "<html><head><meta charset='utf-8'><title>Profiles</title></head><body>"
            f"<h1>Profiling - {dataset} / {table_name}</h1>"
            "<ul>"
            f"{links}"
            "</ul></body></html>"
        )
        (profile_dir / "index.html").write_text(html, encoding="utf-8")

    def _write_dataset_index(self, dataset: str):
        dataset_dir = self.data_lake.resolve_reports_path(dataset)
        table_indexes = sorted(dataset_dir.glob("*/index.html"))
        root_profile_files = sorted(dataset_dir.glob("profiling_*.html"))

        root_links = "\n".join(
            f'<li><a href="{file.name}">{file.stem.replace("profiling_", "Año ")}</a></li>'
            for file in root_profile_files
        )
        table_links = "\n".join(
            f'<li><a href="{index_file.parent.name}/index.html">{index_file.parent.name}</a></li>'
            for index_file in table_indexes
        )

        html = (
            "<html><head><meta charset='utf-8'><title>Dataset Profiles</title></head><body>"
            f"<h1>Profiling - {dataset}</h1>"
            "<h2>Perfiles por año</h2><ul>"
            f"{root_links}"
            "</ul><h2>Tablas</h2><ul>"
            f"{table_links}"
            "</ul></body></html>"
        )
        (dataset_dir / "index.html").write_text(html, encoding="utf-8")
