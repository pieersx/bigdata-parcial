from typing import Any, Dict, List

from app.audit.audit_logger import AuditLogger
from app.audit.control_manager import ControlManager
from app.config.settings import settings
from app.models.audit_schemas import ExecutionStatus
from app.quality.gold_quality_analyzer import GoldQualityAnalyzer
from app.services.gold_transform_service import GoldTransformService
from app.spark.session import SparkSessionFactory
from app.storage.data_lake import DataLake
from app.storage.gold_storage import GoldStorage
from app.utils.logger import StructuredLogger


class GoldPipeline:
    def __init__(self):
        self.logger = StructuredLogger(self.__class__.__name__)
        self.spark = SparkSessionFactory.create({**settings.spark, "app_name": "gold-municipal-parcial"})
        self.data_lake = DataLake(
            raw_path=settings.raw_path,
            bronze_path=settings.bronze_path,
            silver_path=settings.silver_path,
            gold_path=settings.gold_path,
            reports_path=settings.reports_path,
        )
        self.audit_logger = AuditLogger(settings.audit_path)
        self.control_manager = ControlManager(self.audit_logger)

    def run(self) -> Dict[str, Any]:
        execution = self.control_manager.start_execution(
            pipeline_name="gold_municipal_analytics",
            input_parameters={"silver_path": str(settings.silver_path), "gold_path": str(settings.gold_path)},
        )
        gold_config = settings.gold
        storage = GoldStorage(self.data_lake, write_mode=gold_config.get("write_mode", "overwrite"))
        quality = GoldQualityAnalyzer(self.control_manager, settings.quality)
        service = GoldTransformService(
            spark=self.spark,
            storage=storage,
            quality_analyzer=quality,
            execution_id=execution.execution_id,
            silver_path=str(settings.silver_path),
            audit_path=str(settings.audit_path),
            reference_path=str(settings.raw_path.parent / "reference"),
        )
        builders = [
            # Gold es la unica capa que construye modelo dimensional:
            # primero dimensiones, luego hechos y finalmente salidas auxiliares.
            ("dim_municipalidad_gold", service.build_dim_municipalidad_gold),
            ("dim_ubigeo", service.build_dim_ubigeo),
            ("dim_tiempo", service.build_dim_tiempo),
            ("dim_clasificador_ingreso", service.build_dim_clasificador_ingreso),
            ("dim_estado_sismepre", service.build_dim_estado_sismepre),
            ("dim_formulario_sismepre", service.build_dim_formulario_sismepre),
            ("dim_pregunta_sismepre", service.build_dim_pregunta_sismepre),
            ("fact_ingresos_mensuales", service.build_fact_ingresos_mensuales),
            ("fact_ingresos_clasificador", service.build_fact_ingresos_clasificador),
            ("fact_predial_mensual", service.build_fact_predial_mensual),
            ("fact_sismepre_cumplimiento", service.build_fact_sismepre_cumplimiento),
            ("fact_sismepre_respuestas_resumen", service.build_fact_sismepre_respuestas_resumen),
            ("fact_renamu_gestion_tributaria", service.build_fact_renamu_gestion_tributaria),
            ("fact_renamu_software_at", service.build_fact_renamu_software_at),
            # Estas salidas son auxiliares para consumo rapido; para exponer
            # el modelo se deben priorizar las tablas dim_* y fact_*.
            ("mart_dashboard_municipal", service.build_mart_dashboard_municipal),
            ("mart_kpi_resumen_ejecutivo", service.build_mart_kpi_resumen_ejecutivo),
            ("pbi_dashboard_01", service.build_pbi_dashboard_01),
            ("pbi_dashboard_02", service.build_pbi_dashboard_02),
            ("pbi_dashboard_03", service.build_pbi_dashboard_03),
            ("pbi_dashboard_04", service.build_pbi_dashboard_04),
            ("pbi_dashboard_05", service.build_pbi_dashboard_05),
            ("pbi_dashboard_06", service.build_pbi_dashboard_06),
            ("fact_calidad_datos", service.build_fact_calidad_datos),
        ]
        results: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        for table_name, builder in builders:
            try:
                results.append(builder())
            except Exception as error:
                error_details = {
                    "error_type": "GoldTransformError",
                    "error_message": str(error),
                    "table_name": table_name,
                }
                self.control_manager.log_pipeline_error("GoldTransformError", str(error), {"table_name": table_name})
                errors.append(error_details)

        failed_checks = [
            check
            for result in results
            for check in result.get("quality_checks", [])
            if check["status"] == "failed"
        ]
        summary = {
            "total_tables": len(builders),
            "published_tables": len(results),
            "records_published": sum(result["records_published"] for result in results),
            "failed_quality_checks": len(failed_checks),
            "published": results,
            "errors": errors,
        }
        status = (
            ExecutionStatus.FAILED
            if errors and not results
            else ExecutionStatus.PARTIAL
            if errors or failed_checks
            else ExecutionStatus.SUCCESS
        )
        self.audit_logger.log_metrics_snapshot(f"gold_summary_{execution.execution_id}", summary)
        self.control_manager.end_execution(status, summary)
        return {"pipeline": "gold", "execution_id": execution.execution_id, "status": status, **summary}

    def stop(self):
        self.spark.stop()
