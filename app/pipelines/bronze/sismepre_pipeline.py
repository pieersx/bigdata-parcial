from pathlib import Path
from typing import Any, Dict, List

from app.pipelines.bronze.bronze_ingestion_pipeline import BronzeIngestionPipeline


class SismeprePipeline(BronzeIngestionPipeline):
    dataset_name = "sismepre"

    def build_assets(self) -> List[Dict[str, Any]]:
        assets: List[Dict[str, Any]] = []
        year_column = self.dataset_config.get("year_column", "ANO_APLICACION")

        for resource in self.dataset_config.get("diccionarios", []):
            table_name = Path(resource["filename"]).stem
            assets.append(
                self._csv_asset(
                    name=table_name,
                    filename=resource["filename"],
                    url=resource["url"],
                    table_name=table_name,
                    asset_role="reference",
                    source_type="mef_api",
                    required=resource.get("required", True),
                    profiling_enabled=False,
                    quality_enabled=True,
                )
            )

        for resource in self.dataset_config.get("archivos", []):
            table_name = Path(resource["filename"]).stem
            assets.append(
                self._csv_asset(
                    name=table_name,
                    filename=resource["filename"],
                    url=resource["url"],
                    table_name=table_name,
                    asset_role="table",
                    source_type="mef_api",
                    year_column=year_column,
                    profiling_enabled=True,
                    quality_enabled=True,
                )
            )

        return assets
