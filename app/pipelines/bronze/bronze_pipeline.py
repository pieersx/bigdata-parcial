from typing import Any, Dict, List, Optional

from app.audit.audit_logger import AuditLogger
from app.audit.control_manager import ControlManager
from app.clients.download_client import DownloadClient
from app.clients.mef_client import MEFClient
from app.config.settings import settings
from app.models.audit_schemas import ExecutionStatus
from app.pipelines.bronze.categorias_pipeline import CategoriasMunicipalidadesPipeline
from app.pipelines.bronze.ingresos_pipeline import IngresosPipeline
from app.pipelines.bronze.renamu_pipeline import RenamuPipeline
from app.pipelines.bronze.sismepre_pipeline import SismeprePipeline
from app.profiling.profiling_generator import ProfilingGenerator
from app.quality.data_quality_analyzer import DataQualityAnalyzer
from app.services.bronze_transform_service import BronzeTransformService
from app.services.landing_service import LandingService
from app.spark.session import SparkSessionFactory
from app.storage.data_lake import DataLake
from app.storage.parquet_storage import ParquetStorage
from app.utils.logger import StructuredLogger


class BronzePipeline:
    def __init__(self):
        self.logger = StructuredLogger(self.__class__.__name__)
        self.spark = SparkSessionFactory.create(settings.spark)
        self.data_lake = DataLake(
            raw_path=settings.raw_path,
            bronze_path=settings.bronze_path,
            reports_path=settings.reports_path,
        )
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
        self.landing_service = LandingService(
            mef_client=self.mef_client,
            download_client=self.download_client,
            data_lake=self.data_lake,
            control_manager=self.control_manager,
        )

        self.parquet_storage = ParquetStorage(self.data_lake)
        self.quality_analyzer = DataQualityAnalyzer(
            spark=self.spark,
            control_manager=self.control_manager,
            quality_config=settings.quality,
        )
        self.profiling_generator = ProfilingGenerator(
            data_lake=self.data_lake,
            profiling_config=settings.profiling,
        )
        self.bronze_transform_service = BronzeTransformService(
            spark=self.spark,
            parquet_storage=self.parquet_storage,
            quality_analyzer=self.quality_analyzer,
            profiling_generator=self.profiling_generator,
        )

        datasets = settings.datasets
        self.dataset_pipelines = {
            "ingresos": IngresosPipeline(
                self.landing_service,
                self.bronze_transform_service,
                datasets["ingresos"],
                control_manager=self.control_manager,
            ),
            "sismepre": SismeprePipeline(
                self.landing_service,
                self.bronze_transform_service,
                datasets["sismepre"],
                control_manager=self.control_manager,
            ),
            "renamu": RenamuPipeline(
                self.landing_service,
                self.bronze_transform_service,
                datasets["renamu"],
                control_manager=self.control_manager,
            ),
            "categorias_municipalidades": CategoriasMunicipalidadesPipeline(
                self.landing_service,
                self.bronze_transform_service,
                datasets["categorias_municipalidades"],
                control_manager=self.control_manager,
            ),
        }

    def run(self, dataset_names: Optional[List[str]] = None) -> Dict[str, Any]:
        self.data_lake.validate_bronze_contract()
        selected_datasets = dataset_names or list(self.dataset_pipelines.keys())
        execution = self.control_manager.start_execution(
            pipeline_name="bronze_parquet_medallion",
            input_parameters={"datasets": selected_datasets},
        )

        self.logger.info(
            "Bronze parquet pipeline started",
            execution_id=execution.execution_id,
            dataset_count=len(selected_datasets),
        )

        results: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []

        for dataset_name in selected_datasets:
            pipeline = self.dataset_pipelines.get(dataset_name)
            if not pipeline:
                errors.append({"dataset": dataset_name, "error": "Unknown dataset pipeline"})
                continue
            results.extend(pipeline.run())

        asset_errors = [
            {
                "dataset": result["dataset"],
                "asset_name": result["asset_name"],
                "error": result.get("error_message"),
            }
            for result in results
            if result["status"] == "error"
        ]
        errors.extend(asset_errors)

        success_count = sum(1 for result in results if result["status"] == "success")
        skipped_existing = sum(1 for result in results if result["status"] == "skipped_existing")
        skipped_optional = sum(1 for result in results if result["status"] == "skipped_optional")
        failed_count = len(asset_errors)
        failed_quality_checks = [
            quality_check
            for result in results
            for quality_check in result.get("quality_checks", [])
            if quality_check["status"] == "failed"
        ]

        if failed_count == 0 and not errors and not failed_quality_checks:
            final_status = ExecutionStatus.SUCCESS
        elif success_count == 0 and skipped_existing == 0 and skipped_optional == 0:
            final_status = ExecutionStatus.FAILED
        else:
            final_status = ExecutionStatus.PARTIAL

        output_summary = {
            "total_assets": len(results),
            "successful": success_count,
            "skipped_existing": skipped_existing,
            "skipped_optional": skipped_optional,
            "failed": failed_count,
            "failed_quality_checks": len(failed_quality_checks),
            "errors": errors,
            "details": results,
        }

        self.control_manager.end_execution(status=final_status, output_summary=output_summary)
        execution_report = self.control_manager.get_execution_report()

        self.logger.info(
            "Bronze parquet pipeline completed",
            execution_id=execution.execution_id,
            total=len(results),
            success=success_count,
            skipped_existing=skipped_existing,
            skipped_optional=skipped_optional,
            failed=failed_count,
        )

        return {
            "pipeline": "bronze",
            "execution_id": execution.execution_id,
            **output_summary,
            "execution_report": execution_report,
        }

    def stop(self):
        self.spark.stop()
