import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
from app.models.audit_schemas import (
    AuditLog, 
    DataQualityCheck, 
    ExecutionStatus,
    DataQualityStatus
)
from app.utils.logger import StructuredLogger


class AuditLogger:
    """Gestiona auditoría y trazabilidad de ejecuciones"""
    
    def __init__(self, audit_path: Path):
        self.audit_path = audit_path
        self.logger = StructuredLogger(self.__class__.__name__)
        self._ensure_audit_structure()
    
    def _ensure_audit_structure(self):
        """Crea estructura de carpetas para auditoría"""
        directories = [
            self.audit_path / "executions",
            self.audit_path / "quality_checks",
            self.audit_path / "errors",
            self.audit_path / "metrics"
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def log_execution(self, audit_log: AuditLog) -> Path:
        """Registra ejecución de pipeline"""
        date_partition = datetime.now().strftime("%Y/%m/%d")
        output_dir = self.audit_path / "executions" / date_partition
        output_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = output_dir / f"{audit_log.audit_id}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(audit_log.model_dump(mode='json'), f, indent=2, ensure_ascii=False)
        
        self.logger.info(
            "Audit log created",
            audit_id=audit_log.audit_id,
            status=audit_log.status,
            file_path=str(file_path)
        )
        
        return file_path
    
    def log_quality_check(self, quality_check: DataQualityCheck) -> Path:
        """Registra verificación de calidad de datos"""
        date_partition = datetime.now().strftime("%Y/%m/%d")
        output_dir = self.audit_path / "quality_checks" / date_partition
        output_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = output_dir / f"{quality_check.check_id}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(quality_check.model_dump(mode='json'), f, indent=2, ensure_ascii=False)
        
        self.logger.info(
            "Quality check logged",
            check_id=quality_check.check_id,
            status=quality_check.status,
            failure_rate=quality_check.failure_rate
        )
        
        return file_path
    
    def log_error(self, error_details: Dict[str, Any]) -> Path:
        """Registra errores para análisis posterior"""
        date_partition = datetime.now().strftime("%Y/%m/%d")
        output_dir = self.audit_path / "errors" / date_partition
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        file_path = output_dir / f"error_{timestamp}.json"
        
        error_log = {
            "timestamp": datetime.now().isoformat(),
            **error_details
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(error_log, f, indent=2, ensure_ascii=False)

        return file_path

    def log_metrics_snapshot(self, metric_name: str, payload: Dict[str, Any]) -> Path:
        date_partition = datetime.now().strftime("%Y/%m/%d")
        output_dir = self.audit_path / "metrics" / date_partition
        output_dir.mkdir(parents=True, exist_ok=True)

        file_path = output_dir / f"{metric_name}.json"
        with open(file_path, 'w', encoding='utf-8') as file_handle:
            json.dump(payload, file_handle, indent=2, ensure_ascii=False)

        self.logger.info(
            "Metrics snapshot logged",
            metric_name=metric_name,
            file_path=str(file_path),
        )
        return file_path
    
    def get_execution_summary(self, date: datetime = None) -> Dict[str, Any]:
        """Genera resumen de ejecuciones por fecha"""
        if date is None:
            date = datetime.now()
        
        date_partition = date.strftime("%Y/%m/%d")
        execution_dir = self.audit_path / "executions" / date_partition
        
        if not execution_dir.exists():
            return {
                "date": date.isoformat(),
                "total_executions": 0,
                "summary": {}
            }
        
        logs = []
        for file in execution_dir.glob("*.json"):
            with open(file, 'r') as f:
                logs.append(json.load(f))
        
        summary = {
            "date": date.isoformat(),
            "total_executions": len(logs),
            "successful": sum(1 for log in logs if log['status'] == 'success'),
            "failed": sum(1 for log in logs if log['status'] == 'failed'),
            "partial": sum(1 for log in logs if log['status'] == 'partial'),
            "total_records_processed": sum(log.get('records_processed', 0) for log in logs),
            "total_records_failed": sum(log.get('records_failed', 0) for log in logs)
        }
        
        return summary
