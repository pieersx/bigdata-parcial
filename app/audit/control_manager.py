from typing import Dict, Any, List, Optional
from datetime import datetime
from app.models.audit_schemas import (
    ExecutionControl,
    AuditLog,
    DataQualityCheck,
    ExecutionStatus,
    DataQualityStatus
)
from app.audit.audit_logger import AuditLogger
from app.utils.logger import StructuredLogger


class ControlManager:
    """Gestiona control de ejecuciones y calidad de datos"""
    
    def __init__(self, audit_logger: AuditLogger):
        self.audit_logger = audit_logger
        self.logger = StructuredLogger(self.__class__.__name__)
        self.current_execution: Optional[ExecutionControl] = None
    
    def start_execution(
        self,
        pipeline_name: str,
        input_parameters: Dict[str, Any]
    ) -> ExecutionControl:
        """Inicia control de ejecución"""
        self.current_execution = ExecutionControl(
            pipeline_name=pipeline_name,
            input_parameters=input_parameters
        )
        
        self.logger.info(
            "Execution started",
            execution_id=self.current_execution.execution_id,
            pipeline=pipeline_name
        )
        
        return self.current_execution
    
    def end_execution(
        self,
        status: ExecutionStatus,
        output_summary: Dict[str, Any]
    ) -> ExecutionControl:
        """Finaliza control de ejecución"""
        if not self.current_execution:
            raise ValueError("No active execution to end")
        
        self.current_execution.end_time = datetime.now()
        self.current_execution.status = status
        self.current_execution.output_summary = output_summary
        
        # Calcular duración
        duration = (
            self.current_execution.end_time - 
            self.current_execution.start_time
        ).total_seconds()
        
        processed_assets = output_summary.get('total_assets', output_summary.get('total_series', 0))
        successful_assets = output_summary.get('successful', 0) + output_summary.get('skipped_existing', 0)
        failed_assets = output_summary.get('failed', 0)

        # Crear audit log
        audit_log = AuditLog(
            pipeline_name=self.current_execution.pipeline_name,
            execution_id=self.current_execution.execution_id,
            status=status,
            duration_seconds=duration,
            records_processed=processed_assets,
            records_success=successful_assets,
            records_failed=failed_assets,
            error_details=output_summary.get('errors', []),
            metadata=self.current_execution.input_parameters
        )
        
        # Registrar en auditoría
        self.audit_logger.log_execution(audit_log)
        
        self.current_execution.audit_logs.append(audit_log)
        
        self.logger.info(
            "Execution ended",
            execution_id=self.current_execution.execution_id,
            status=status,
            duration_seconds=duration
        )
        
        return self.current_execution
    
    def perform_quality_check(
        self,
        check_name: str,
        check_type: str,
        dataset: str,
        data: List[Dict[str, Any]],
        validation_func: callable
    ) -> DataQualityCheck:
        """Ejecuta verificación de calidad de datos"""
        
        records_checked = len(data)
        validation_results = [validation_func(record) for record in data]
        records_passed = sum(validation_results)
        records_failed = records_checked - records_passed
        failure_rate = records_failed / records_checked if records_checked > 0 else 0
        
        # Determinar estado
        if failure_rate == 0:
            status = DataQualityStatus.PASSED
        elif failure_rate < 0.05:  # 5% threshold
            status = DataQualityStatus.WARNING
        else:
            status = DataQualityStatus.FAILED
        
        check_id = f"{check_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        quality_check = DataQualityCheck(
            check_id=check_id,
            check_name=check_name,
            check_type=check_type,
            status=status,
            dataset=dataset,
            records_checked=records_checked,
            records_passed=records_passed,
            records_failed=records_failed,
            failure_rate=failure_rate,
            details={
                "threshold": 0.05,
                "validation_criteria": validation_func.__name__
            }
        )
        
        # Registrar verificación
        self.audit_logger.log_quality_check(quality_check)
        
        if self.current_execution:
            self.current_execution.quality_checks.append(quality_check)
        
        self.logger.info(
            "Quality check performed",
            check_name=check_name,
            status=status,
            failure_rate=failure_rate
        )
        
        return quality_check
    
    def log_pipeline_error(
        self,
        error_type: str,
        error_message: str,
        context: Dict[str, Any]
    ):
        """Registra error en pipeline"""
        error_details = {
            "execution_id": self.current_execution.execution_id if self.current_execution else "unknown",
            "error_type": error_type,
            "error_message": error_message,
            "context": context
        }
        
        self.audit_logger.log_error(error_details)
        
        self.logger.error(
            "Pipeline error logged",
            error_type=error_type,
            error_message=error_message
        )
    
    def get_execution_report(self) -> Dict[str, Any]:
        """Genera reporte de ejecución actual"""
        if not self.current_execution:
            return {"error": "No active execution"}
        
        return {
            "execution_id": self.current_execution.execution_id,
            "pipeline": self.current_execution.pipeline_name,
            "status": self.current_execution.status,
            "start_time": self.current_execution.start_time.isoformat(),
            "end_time": self.current_execution.end_time.isoformat() if self.current_execution.end_time else None,
            "duration_seconds": (
                (self.current_execution.end_time - self.current_execution.start_time).total_seconds()
                if self.current_execution.end_time else None
            ),
            "input_parameters": self.current_execution.input_parameters,
            "output_summary": self.current_execution.output_summary,
            "quality_checks_count": len(self.current_execution.quality_checks),
            "quality_checks_passed": sum(
                1 for qc in self.current_execution.quality_checks 
                if qc.status == DataQualityStatus.PASSED
            ),
            "audit_logs_count": len(self.current_execution.audit_logs)
        }
