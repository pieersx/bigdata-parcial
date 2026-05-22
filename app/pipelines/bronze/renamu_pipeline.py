from typing import Any, Dict, List

from app.services.ingestion_service import IngestionService


class RenamuPipeline:
    def __init__(self, ingestion_service: IngestionService, dataset_config: Dict[str, Any]):
        self.ingestion_service = ingestion_service
        self.dataset_config = dataset_config

    def run(self) -> List[Dict[str, Any]]:
        results = []

        for asset_name, resource in self.dataset_config.items():
            results.append(
                self.ingestion_service.ingest_resource(
                    dataset="renamu",
                    asset_name=asset_name,
                    resource=resource,
                )
            )

        return results
