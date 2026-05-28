from pathlib import Path
from typing import Any, Dict, List

from app.pipelines.bronze.bronze_ingestion_pipeline import BronzeIngestionPipeline


class IngresosPipeline(BronzeIngestionPipeline):
    dataset_name = "ingresos"

    def build_assets(self) -> List[Dict[str, Any]]:
        assets = [
            self._csv_asset(
                name="ingresos_diccionario",
                filename=self.dataset_config["diccionario"]["filename"],
                url=self.dataset_config["diccionario"]["url"],
                table_name="ingresos_diccionario",
                asset_role="reference",
                source_type="direct_download",
                profiling_enabled=False,
                quality_enabled=True,
            )
        ]

        table_name = self.dataset_config.get("table_name", "ingresos")
        year_column = self.dataset_config.get("year_column", "ANO_DOC")

        for resource in self.dataset_config.get("historico", []):
            assets.append(
                self._csv_asset(
                    name=f"ingresos_{resource['anio']}",
                    filename=resource["filename"],
                    url=resource["url"],
                    table_name=table_name,
                    asset_role="table",
                    source_type="direct_download",
                    year_column=year_column,
                    default_year=resource.get("anio"),
                    profiling_enabled=True,
                    quality_enabled=True,
                )
            )

        for resource in self.dataset_config.get("api", []):
            assets.append(
                self._csv_asset(
                    name=f"ingresos_{resource['anio']}_api",
                    filename=resource["filename"],
                    url=resource["url"],
                    table_name=table_name,
                    asset_role="table",
                    source_type="mef_api",
                    year_column=year_column,
                    default_year=resource.get("anio"),
                    profiling_enabled=True,
                    quality_enabled=True,
                )
            )

        return assets
