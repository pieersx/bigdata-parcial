from typing import Any, Dict, List

from app.pipelines.bronze.bronze_ingestion_pipeline import BronzeIngestionPipeline


class CategoriasMunicipalidadesPipeline(BronzeIngestionPipeline):
    dataset_name = "categorias_municipalidades"

    def build_assets(self) -> List[Dict[str, Any]]:
        return [
            self._csv_asset(
                name="categorias_municipalidades",
                filename=self.dataset_config["filename"],
                url=self.dataset_config.get("url", "local_raw"),
                table_name=self.dataset_config.get("table_name", "categorias_municipalidades"),
                asset_role="table",
                source_type="local_raw",
                profiling_enabled=True,
                quality_enabled=True,
            )
        ]
