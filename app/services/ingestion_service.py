import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.audit.control_manager import ControlManager
from app.clients.download_client import DownloadClient
from app.clients.mef_client import MEFClient
from app.models.schemas import ResourceMetadata
from app.storage.data_lake import DataLake
from app.utils.logger import StructuredLogger


class IngestionService:
    def __init__(
        self,
        mef_client: MEFClient,
        download_client: DownloadClient,
        data_lake: DataLake,
        control_manager: Optional[ControlManager] = None
    ):
        self.mef_client = mef_client
        self.download_client = download_client
        self.data_lake = data_lake
        self.control_manager = control_manager
        self.logger = StructuredLogger(self.__class__.__name__)

    def ingest_resource(
        self,
        dataset: str,
        asset_name: str,
        resource: Dict[str, Any]
    ) -> Dict[str, Any]:
        url = resource['url']
        if "api.datosabiertos.mef.gob.pe/DatosAbiertos/v1/datastore_search" in url:
            return self.ingest_api_resource(dataset, asset_name, resource)
        return self.ingest_direct_resource(dataset, asset_name, resource)

    def ingest_direct_resource(
        self,
        dataset: str,
        asset_name: str,
        resource: Dict[str, Any]
    ) -> Dict[str, Any]:
        filename = resource['filename']
        url = resource['url']
        target_path = self.data_lake.resolve_bronze_file_path(dataset, filename)

        if target_path.exists() and target_path.stat().st_size > 0:
            self._run_file_quality_check(dataset, filename, target_path)
            return self._build_result(
                dataset=dataset,
                asset_name=asset_name,
                source_type="direct_download",
                source_url=url,
                status="skipped_existing",
                file_path=target_path,
                size_bytes=target_path.stat().st_size,
                skipped=True,
            )

        start_time = time.time()

        try:
            size_bytes = self.download_client.download_to_path(url, target_path)
            execution_time = (time.time() - start_time) * 1000
            self._run_file_quality_check(dataset, filename, target_path)

            self.logger.info(
                "Direct resource ingested",
                dataset=dataset,
                asset_name=asset_name,
                file_path=str(target_path),
            )
            return self._build_result(
                dataset=dataset,
                asset_name=asset_name,
                source_type="direct_download",
                source_url=url,
                status="success",
                file_path=target_path,
                size_bytes=size_bytes,
                execution_time_ms=execution_time,
            )
        except Exception as error:
            self._log_ingestion_error(dataset, asset_name, error)
            return self._build_result(
                dataset=dataset,
                asset_name=asset_name,
                source_type="direct_download",
                source_url=url,
                status="error",
                error_message=str(error),
            )

    def ingest_api_resource(
        self,
        dataset: str,
        asset_name: str,
        resource: Dict[str, Any]
    ) -> Dict[str, Any]:
        filename = resource['filename']
        url = resource['url']
        target_path = self.data_lake.resolve_bronze_file_path(dataset, filename)

        if target_path.exists() and target_path.stat().st_size > 0:
            self._run_file_quality_check(dataset, filename, target_path)
            return self._build_result(
                dataset=dataset,
                asset_name=asset_name,
                source_type="mef_api",
                source_url=url,
                status="skipped_existing",
                file_path=target_path,
                size_bytes=target_path.stat().st_size,
                skipped=True,
            )

        start_time = time.time()
        first_page: Optional[List[Dict[str, Any]]] = None

        try:
            def pages():
                nonlocal first_page
                for records in self.mef_client.iter_records(url):
                    if first_page is None:
                        first_page = records
                    yield records

            file_path, records_count = self.data_lake.write_csv_pages(dataset, filename, pages())
            execution_time = (time.time() - start_time) * 1000

            if first_page:
                self._run_api_quality_check(dataset, asset_name, first_page)

            self.logger.info(
                "API resource ingested",
                dataset=dataset,
                asset_name=asset_name,
                file_path=str(file_path),
                records_count=records_count,
            )
            return self._build_result(
                dataset=dataset,
                asset_name=asset_name,
                source_type="mef_api",
                source_url=url,
                status="success",
                file_path=file_path,
                size_bytes=file_path.stat().st_size,
                records_count=records_count,
                execution_time_ms=execution_time,
            )
        except Exception as error:
            self._log_ingestion_error(dataset, asset_name, error)
            return self._build_result(
                dataset=dataset,
                asset_name=asset_name,
                source_type="mef_api",
                source_url=url,
                status="error",
                error_message=str(error),
            )

    def _run_file_quality_check(self, dataset: str, filename: str, file_path: Path):
        if not self.control_manager:
            return

        def validate_file(record: Dict[str, Any]) -> bool:
            return record['size_bytes'] > 0

        self.control_manager.perform_quality_check(
            check_name=f"file_non_empty_{dataset}_{filename.replace('.', '_')}",
            check_type="completeness",
            dataset=dataset,
            data=[{"size_bytes": file_path.stat().st_size}],
            validation_func=validate_file,
        )

    def _run_api_quality_check(self, dataset: str, asset_name: str, records: List[Dict[str, Any]]):
        if not self.control_manager:
            return

        def validate_record(record: Dict[str, Any]) -> bool:
            return bool(record) and any(value not in (None, "") for value in record.values())

        self.control_manager.perform_quality_check(
            check_name=f"api_structure_{dataset}_{asset_name}",
            check_type="completeness",
            dataset=dataset,
            data=records,
            validation_func=validate_record,
        )

    def _build_result(
        self,
        dataset: str,
        asset_name: str,
        source_type: str,
        source_url: str,
        status: str,
        file_path: Optional[Path] = None,
        size_bytes: Optional[int] = None,
        records_count: Optional[int] = None,
        execution_time_ms: Optional[float] = None,
        error_message: Optional[str] = None,
        skipped: bool = False,
    ) -> Dict[str, Any]:
        metadata = ResourceMetadata(
            dataset=dataset,
            asset_name=asset_name,
            source_type=source_type,
            source_url=source_url,
            status=status,
            skipped=skipped,
            file_path=str(file_path) if file_path else None,
            size_bytes=size_bytes,
            records_count=records_count,
            execution_time_ms=execution_time_ms,
            error_message=error_message,
        )
        return metadata.model_dump(mode='json')

    def _log_ingestion_error(self, dataset: str, asset_name: str, error: Exception):
        self.logger.error(
            "Resource ingestion failed",
            dataset=dataset,
            asset_name=asset_name,
            error=str(error),
        )

        if self.control_manager:
            self.control_manager.log_pipeline_error(
                error_type="IngestionError",
                error_message=str(error),
                context={
                    "dataset": dataset,
                    "asset_name": asset_name,
                },
            )
