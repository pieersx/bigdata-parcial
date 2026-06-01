from pathlib import Path
from typing import Any, Dict, List

from app.audit.audit_logger import AuditLogger
from app.audit.control_manager import ControlManager
from app.config.settings import settings
from app.models.audit_schemas import ExecutionStatus
from app.quality.silver_quality_analyzer import SilverQualityAnalyzer
from app.services.silver_transform_service import SilverTransformService
from app.spark.session import SparkSessionFactory
from app.storage.data_lake import DataLake
from app.storage.silver_storage import SilverStorage
from app.utils.logger import StructuredLogger


class SilverPipeline:
    def __init__(self):
        self.logger = StructuredLogger(self.__class__.__name__)
        self.spark = SparkSessionFactory.create({**settings.spark, "app_name": "silver-municipal-parcial"})
        self.data_lake = DataLake(
            raw_path=settings.raw_path,
            bronze_path=settings.bronze_path,
            silver_path=settings.silver_path,
            reports_path=settings.reports_path,
        )
        self.audit_logger = AuditLogger(settings.audit_path)
        self.control_manager = ControlManager(self.audit_logger)

    def run(self) -> Dict[str, Any]:
        execution = self.control_manager.start_execution(
            pipeline_name="silver_municipal_curated",
            input_parameters={"bronze_path": str(settings.bronze_path), "silver_path": str(settings.silver_path)},
        )
        silver_config = settings.silver
        storage = SilverStorage(self.data_lake, write_mode=silver_config.get("write_mode", "overwrite"))
        quality = SilverQualityAnalyzer(self.control_manager, settings.quality)
        service = SilverTransformService(
            spark=self.spark,
            storage=storage,
            quality_analyzer=quality,
            execution_id=execution.execution_id,
            bronze_path=str(settings.bronze_path),
            required_government_level=silver_config.get("required_government_level", "M"),
            quarantine_enabled=bool(silver_config.get("quarantine_enabled", True)),
        )
        builders = [
            ("dim_municipalidad", service.build_dim_municipalidad),
            ("fact_ingresos_municipales", service.build_fact_ingresos_municipales),
            ("fact_predial_esat", service.build_fact_predial_esat),
            ("fact_sismepre_respuestas", service.build_fact_sismepre_respuestas),
            ("dim_sismepre_entidad_estado", lambda: service.build_simple_dimension(
                "dim_sismepre_entidad_estado",
                "sismepre/rentas_entidad_estado",
                ["SEC_EJEC", "ANO_APLICACION", "PERIODO", "ESTADO", "CLASIFICACION"],
                ["SEC_EJEC", "ANO_APLICACION", "PERIODO"],
            )),
            ("dim_sismepre_pregunta", lambda: service.build_simple_dimension(
                "dim_sismepre_pregunta",
                "sismepre/rentas_preguntas",
                ["ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID", "DESCRIPCION"],
                ["ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID"],
            )),
            ("dim_sismepre_formulario", lambda: service.build_simple_dimension(
                "dim_sismepre_formulario",
                "sismepre/rentas_formulario",
                ["ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "TITULO"],
                ["ANO_APLICACION", "PERIODO", "FORMULARIO_ID"],
            )),
        ]
        results: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        for table_name, builder in builders:
            try:
                results.append(builder())
            except Exception as error:
                error_details = {
                    "error_type": "SilverTransformError",
                    "error_message": str(error),
                    "table_name": table_name,
                }
                self.control_manager.log_pipeline_error("SilverTransformError", str(error), {"table_name": table_name})
                errors.append(error_details)

        published = [result for result in results if result["status"] == "published"]
        blocked = [result for result in results if result["status"] == "blocked"]
        failed_checks = [
            check
            for result in published
            for check in result.get("quality_checks", [])
            if check["status"] == "failed"
        ]
        summary = {
            "total_tables": len(builders),
            "published_tables": len(published),
            "blocked_tables": len(blocked),
            "records_published": sum(result["records_published"] for result in published),
            "records_quarantined": sum(result["records_quarantined"] for result in published),
            "failed_quality_checks": len(failed_checks),
            "published": published,
            "blocked": blocked,
            "errors": errors,
        }
        status = (
            ExecutionStatus.FAILED
            if errors and not published
            else ExecutionStatus.PARTIAL
            if errors or blocked or failed_checks
            else ExecutionStatus.SUCCESS
        )
        self.audit_logger.log_metrics_snapshot(f"silver_summary_{execution.execution_id}", summary)
        self.control_manager.end_execution(status, summary)
        return {"pipeline": "silver", "execution_id": execution.execution_id, "status": status, **summary}

    def stop(self):
        self.spark.stop()
