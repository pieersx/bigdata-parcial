from pathlib import Path
from typing import Any, Dict, List

from app.pipelines.bronze.bronze_ingestion_pipeline import BronzeIngestionPipeline


class RenamuPipeline(BronzeIngestionPipeline):
    dataset_name = "renamu"

    def build_assets(self) -> List[Dict[str, Any]]:
        archive_resource = self.dataset_config["data_completa"]
        pdf_resource = self.dataset_config["diccionario"]
        table_name = self.dataset_config.get("table_name", "renamu")
        year_column = self.dataset_config.get("year_column", "Año")

        return [
            {
                "name": "renamu_diccionario_2022_pdf",
                "filename": pdf_resource["filename"],
                "url": pdf_resource["url"],
                "table_name": "renamu_diccionario_2022_pdf",
                "asset_role": "binary_reference",
                "source_type": "direct_download",
                "read_options": {},
                "required": True,
                "profiling_enabled": False,
                "quality_enabled": False,
                "bronze_enabled": False,
            },
            {
                "name": "renamu_2022_zip",
                "filename": archive_resource["filename"],
                "url": archive_resource["url"],
                "table_name": "renamu_2022_zip",
                "asset_role": "archive",
                "source_type": "direct_download",
                "read_options": {},
                "required": True,
                "profiling_enabled": False,
                "quality_enabled": False,
                "bronze_enabled": False,
            },
            {
                "name": "renamu_2022",
                "filename": "Base_RENAMU_2022_f.csv",
                "table_name": table_name,
                "asset_role": "table",
                "source_type": "extracted_csv",
                "archive_filename": archive_resource["filename"],
                "archive_member_name": "Base_RENAMU_2022_f.csv",
                "read_options": self.dataset_config.get("read_options", {}),
                "year_column": year_column,
                "default_year": 2022,
                "required": True,
                "profiling_enabled": True,
                "quality_enabled": True,
                "bronze_enabled": True,
                "url": archive_resource["url"],
            },
        ]
