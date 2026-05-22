from typing import Any, Dict, List

from app.services.ingestion_service import IngestionService


class SismeprePipeline:
    def __init__(self, ingestion_service: IngestionService, dataset_config: Dict[str, Any]):
        self.ingestion_service = ingestion_service
        self.dataset_config = dataset_config

    def run(self) -> List[Dict[str, Any]]:
        results = []

        for resource in self.dataset_config.get('diccionarios', []):
            results.append(
                self.ingestion_service.ingest_resource(
                    dataset="sismepre",
                    asset_name=resource['filename'],
                    resource=resource,
                )
            )

        for resource in self.dataset_config.get('archivos', []):
            results.append(
                self.ingestion_service.ingest_resource(
                    dataset="sismepre",
                    asset_name=resource['filename'],
                    resource=resource,
                )
            )

        return results
