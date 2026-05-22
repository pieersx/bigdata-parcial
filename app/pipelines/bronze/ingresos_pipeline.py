from typing import Any, Dict, List

from app.services.ingestion_service import IngestionService


class IngresosPipeline:
    def __init__(self, ingestion_service: IngestionService, dataset_config: Dict[str, Any]):
        self.ingestion_service = ingestion_service
        self.dataset_config = dataset_config

    def run(self) -> List[Dict[str, Any]]:
        results = [
            self.ingestion_service.ingest_resource(
                dataset="ingresos",
                asset_name="diccionario",
                resource=self.dataset_config['diccionario'],
            )
        ]

        for resource in self.dataset_config.get('historico', []):
            results.append(
                self.ingestion_service.ingest_resource(
                    dataset="ingresos",
                    asset_name=f"historico_{resource['anio']}",
                    resource=resource,
                )
            )

        for resource in self.dataset_config.get('api', []):
            results.append(
                self.ingestion_service.ingest_resource(
                    dataset="ingresos",
                    asset_name=f"api_{resource['anio']}",
                    resource=resource,
                )
            )

        return results
