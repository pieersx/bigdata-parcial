from typing import Any, Dict, List, Optional

from app.audit.audit_logger import AuditLogger
from app.audit.control_manager import ControlManager
from app.clients.download_client import DownloadClient
from app.clients.mef_client import MEFClient
from app.config.settings import settings
from app.models.audit_schemas import ExecutionStatus
from app.pipelines.bronze.ingresos_pipeline import IngresosPipeline
from app.pipelines.bronze.renamu_pipeline import RenamuPipeline
from app.pipelines.bronze.sismepre_pipeline import SismeprePipeline
from app.services.ingestion_service import IngestionService
from app.storage.data_lake import DataLake
from app.utils.logger import StructuredLogger


class BronzePipeline:
    def __init__(self):
        self.logger = StructuredLogger(self.__class__.__name__)
        self.data_lake = DataLake(base_path=settings.bronze_path)
        self.audit_logger = AuditLogger(audit_path=settings.audit_path)
        self.control_manager = ControlManager(audit_logger=self.audit_logger)

        self.mef_client = MEFClient(
            base_url=settings.mef_base_url,
            timeout=settings.timeout,
            max_retries=settings.max_retries,
            page_size=settings.api_page_size,
        )
        self.download_client = DownloadClient(
            timeout=settings.timeout,
            max_retries=settings.max_retries,
        )

        self.ingestion_service = IngestionService(
            mef_client=self.mef_client,
            download_client=self.download_client,
            data_lake=self.data_lake,
            control_manager=self.control_manager,
        )

        datasets = settings.datasets
        self.dataset_pipelines = {
            "ingresos": IngresosPipeline(self.ingestion_service, datasets['ingresos']),
            "sismepre": SismeprePipeline(self.ingestion_service, datasets['sismepre']),
            "renamu": RenamuPipeline(self.ingestion_service, datasets['renamu']),
        }

    def run(self, dataset_names: Optional[List[str]] = None) -> Dict[str, Any]:
        selected_datasets = dataset_names or list(self.dataset_pipelines.keys())

        execution = self.control_manager.start_execution(
            pipeline_name="bronze_partial_ingestion",
            input_parameters={"datasets": selected_datasets},
        )

        self.logger.info(
            "Bronze pipeline started",
            execution_id=execution.execution_id,
            dataset_count=len(selected_datasets),
        )

        results = []
        errors = []

        for dataset_name in selected_datasets:
            pipeline = self.dataset_pipelines.get(dataset_name)
            if not pipeline:
                errors.append({"dataset": dataset_name, "error": "Unknown dataset pipeline"})
                continue

            dataset_results = pipeline.run()
            results.extend(dataset_results)

        asset_errors = [
            {
                "dataset": result['dataset'],
                "asset_name": result['asset_name'],
                "error": result.get('error_message'),
            }
            for result in results
            if result['status'] == 'error'
        ]
        errors.extend(asset_errors)

        success_count = sum(1 for result in results if result['status'] == 'success')
        skipped_existing = sum(1 for result in results if result['status'] == 'skipped_existing')
        failed_count = len(asset_errors)

        if failed_count == 0 and not errors:
            final_status = ExecutionStatus.SUCCESS
        elif success_count == 0 and skipped_existing == 0:
            final_status = ExecutionStatus.FAILED
        else:
            final_status = ExecutionStatus.PARTIAL

        output_summary = {
            "total_assets": len(results),
            "successful": success_count,
            "skipped_existing": skipped_existing,
            "failed": failed_count,
            "errors": errors,
            "details": results,
        }

        self.control_manager.end_execution(status=final_status, output_summary=output_summary)
        execution_report = self.control_manager.get_execution_report()

        self.logger.info(
            "Bronze pipeline completed",
            execution_id=execution.execution_id,
            total=len(results),
            success=success_count,
            skipped=skipped_existing,
            failed=failed_count,
        )

        return {
            "pipeline": "bronze",
            "execution_id": execution.execution_id,
            **output_summary,
            "execution_report": execution_report,
        }
