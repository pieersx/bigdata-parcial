import shutil
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.audit.control_manager import ControlManager
from app.clients.download_client import DownloadClient
from app.clients.mef_client import MEFClient
from app.storage.data_lake import DataLake
from app.utils.logger import StructuredLogger


class LandingService:
    def __init__(
        self,
        mef_client: MEFClient,
        download_client: DownloadClient,
        data_lake: DataLake,
        control_manager: Optional[ControlManager] = None,
    ):
        self.mef_client = mef_client
        self.download_client = download_client
        self.data_lake = data_lake
        self.control_manager = control_manager
        self.logger = StructuredLogger(self.__class__.__name__)

    def fetch(self, dataset: str, asset: Dict[str, Any]) -> Dict[str, Any]:
        source_type = asset["source_type"]
        if source_type == "direct_download":
            return self._fetch_direct_download(dataset, asset)
        if source_type == "mef_api":
            return self._fetch_api_resource(dataset, asset)
        if source_type == "extracted_csv":
            return self._fetch_extracted_csv(dataset, asset)
        raise ValueError(f"Unsupported source_type: {source_type}")

    def _fetch_direct_download(self, dataset: str, asset: Dict[str, Any]) -> Dict[str, Any]:
        filename = asset["filename"]
        source_url = asset["url"]
        existing_path = self.data_lake.resolve_existing_raw_path(dataset, filename)
        if existing_path and existing_path.stat().st_size > 0:
            materialized_path = self._materialize_raw_copy(dataset, filename, existing_path)
            self._record_file_availability_check(dataset, asset["name"], materialized_path)
            return self._build_result(
                dataset=dataset,
                asset=asset,
                status="skipped_existing",
                raw_path=materialized_path,
                size_bytes=materialized_path.stat().st_size,
                source_url=source_url,
                skipped=True,
            )

        target_path = self.data_lake.resolve_raw_file_path(dataset, filename)
        start_time = time.time()
        try:
            size_bytes = self.download_client.download_to_path(source_url, target_path)
            execution_time_ms = (time.time() - start_time) * 1000
            self._record_file_availability_check(dataset, asset["name"], target_path)
            return self._build_result(
                dataset=dataset,
                asset=asset,
                status="success",
                raw_path=target_path,
                size_bytes=size_bytes,
                execution_time_ms=execution_time_ms,
                source_url=source_url,
            )
        except Exception as error:
            return self._handle_fetch_error(dataset, asset, error)

    def _fetch_api_resource(self, dataset: str, asset: Dict[str, Any]) -> Dict[str, Any]:
        filename = asset["filename"]
        source_url = asset["url"]
        existing_path = self.data_lake.resolve_existing_raw_path(dataset, filename)
        if existing_path and existing_path.stat().st_size > 0:
            materialized_path = self._materialize_raw_copy(dataset, filename, existing_path)
            self._record_file_availability_check(dataset, asset["name"], materialized_path)
            return self._build_result(
                dataset=dataset,
                asset=asset,
                status="skipped_existing",
                raw_path=materialized_path,
                size_bytes=materialized_path.stat().st_size,
                source_url=source_url,
                skipped=True,
            )

        start_time = time.time()
        first_page: Optional[List[Dict[str, Any]]] = None

        try:
            def pages():
                nonlocal first_page
                for records in self.mef_client.iter_records(source_url):
                    if first_page is None:
                        first_page = records
                    yield records

            file_path, records_count = self.data_lake.write_raw_csv_pages(dataset, filename, pages())
            execution_time_ms = (time.time() - start_time) * 1000

            if first_page:
                self._record_api_structure_check(dataset, asset["name"], first_page)

            return self._build_result(
                dataset=dataset,
                asset=asset,
                status="success",
                raw_path=file_path,
                size_bytes=file_path.stat().st_size,
                execution_time_ms=execution_time_ms,
                records_count=records_count,
                source_url=source_url,
            )
        except ValueError as error:
            if not asset.get("required", True):
                self.logger.warning(
                    "Optional API asset returned no rows",
                    dataset=dataset,
                    asset_name=asset["name"],
                    source_url=source_url,
                )
                return self._build_result(
                    dataset=dataset,
                    asset=asset,
                    status="skipped_optional",
                    source_url=source_url,
                    skipped=True,
                    error_message=str(error),
                )
            return self._handle_fetch_error(dataset, asset, error)
        except Exception as error:
            return self._handle_fetch_error(dataset, asset, error)

    def _fetch_extracted_csv(self, dataset: str, asset: Dict[str, Any]) -> Dict[str, Any]:
        filename = asset["filename"]
        existing_path = self.data_lake.resolve_existing_raw_path(dataset, filename)
        if existing_path and existing_path.stat().st_size > 0:
            materialized_path = self._materialize_raw_copy(dataset, filename, existing_path)
            self._record_file_availability_check(dataset, asset["name"], materialized_path)
            return self._build_result(
                dataset=dataset,
                asset=asset,
                status="skipped_existing",
                raw_path=materialized_path,
                size_bytes=materialized_path.stat().st_size,
                source_url=asset.get("url", "archive_extraction"),
                skipped=True,
            )

        archive_path = self.data_lake.resolve_existing_raw_path(dataset, asset["archive_filename"])
        if not archive_path:
            return self._handle_fetch_error(
                dataset,
                asset,
                FileNotFoundError(f"Archive not found for extraction: {asset['archive_filename']}"),
            )

        target_path = self.data_lake.resolve_raw_file_path(dataset, filename)
        start_time = time.time()
        try:
            member_name = self._resolve_archive_member(archive_path, asset["archive_member_name"])
            with zipfile.ZipFile(archive_path) as archive_handle:
                with archive_handle.open(member_name) as source, open(target_path, 'wb') as destination:
                    shutil.copyfileobj(source, destination)

            execution_time_ms = (time.time() - start_time) * 1000
            self._record_file_availability_check(dataset, asset["name"], target_path)
            return self._build_result(
                dataset=dataset,
                asset=asset,
                status="success",
                raw_path=target_path,
                size_bytes=target_path.stat().st_size,
                execution_time_ms=execution_time_ms,
                source_url=str(archive_path),
            )
        except Exception as error:
            return self._handle_fetch_error(dataset, asset, error)

    def _resolve_archive_member(self, archive_path: Path, expected_name: str) -> str:
        with zipfile.ZipFile(archive_path) as archive_handle:
            for member_name in archive_handle.namelist():
                if member_name.endswith(expected_name):
                    return member_name
        raise FileNotFoundError(f"Member {expected_name} not found inside {archive_path}")

    def _materialize_raw_copy(self, dataset: str, filename: str, source_path: Path) -> Path:
        raw_target = self.data_lake.resolve_raw_file_path(dataset, filename)
        if source_path == raw_target:
            return source_path
        if not raw_target.exists() or raw_target.stat().st_size == 0:
            shutil.copy2(source_path, raw_target)
            self.logger.info(
                "Source copied to raw landing",
                dataset=dataset,
                source_path=str(source_path),
                raw_target=str(raw_target),
            )
        return raw_target

    def _record_file_availability_check(self, dataset: str, asset_name: str, file_path: Path):
        if not self.control_manager:
            return

        def validate_file(record: Dict[str, Any]) -> bool:
            return record["size_bytes"] > 0

        self.control_manager.perform_quality_check(
            check_name=f"raw_file_non_empty_{dataset}_{asset_name}",
            check_type="availability",
            dataset=dataset,
            data=[{"size_bytes": file_path.stat().st_size}],
            validation_func=validate_file,
        )

    def _record_api_structure_check(self, dataset: str, asset_name: str, records: List[Dict[str, Any]]):
        if not self.control_manager:
            return

        def validate_record(record: Dict[str, Any]) -> bool:
            return bool(record) and any(value not in (None, "", " ") for value in record.values())

        self.control_manager.perform_quality_check(
            check_name=f"raw_api_structure_{dataset}_{asset_name}",
            check_type="availability",
            dataset=dataset,
            data=records,
            validation_func=validate_record,
        )

    def _handle_fetch_error(self, dataset: str, asset: Dict[str, Any], error: Exception) -> Dict[str, Any]:
        self.logger.error(
            "Landing fetch failed",
            dataset=dataset,
            asset_name=asset["name"],
            error=str(error),
        )
        if self.control_manager:
            self.control_manager.log_pipeline_error(
                error_type="LandingError",
                error_message=str(error),
                context={"dataset": dataset, "asset_name": asset["name"]},
            )
        return self._build_result(
            dataset=dataset,
            asset=asset,
            status="error",
            source_url=asset.get("url", ""),
            error_message=str(error),
        )

    def _build_result(
        self,
        dataset: str,
        asset: Dict[str, Any],
        status: str,
        source_url: str,
        raw_path: Optional[Path] = None,
        size_bytes: Optional[int] = None,
        execution_time_ms: Optional[float] = None,
        records_count: Optional[int] = None,
        skipped: bool = False,
        error_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "dataset": dataset,
            "asset_name": asset["name"],
            "asset_role": asset["asset_role"],
            "table_name": asset.get("table_name"),
            "source_type": asset["source_type"],
            "source_url": source_url,
            "status": status,
            "skipped": skipped,
            "raw_path": str(raw_path) if raw_path else None,
            "size_bytes": size_bytes,
            "records_count": records_count,
            "execution_time_ms": execution_time_ms,
            "error_message": error_message,
        }
