from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum


class ExecutionStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"
    RUNNING = "running"


class DataQualityStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"


class AuditLog(BaseModel):
    audit_id: str = Field(default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S_%f"))
    pipeline_name: str
    execution_id: str
    timestamp: datetime = Field(default_factory=datetime.now)
    status: ExecutionStatus
    duration_seconds: Optional[float] = None
    records_processed: int = 0
    records_success: int = 0
    records_failed: int = 0
    error_details: Optional[List[Dict[str, Any]]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DataQualityCheck(BaseModel):
    check_id: str
    check_name: str
    check_type: str  # completeness, accuracy, consistency, timeliness
    status: DataQualityStatus
    timestamp: datetime = Field(default_factory=datetime.now)
    dataset: str
    records_checked: int
    records_passed: int
    records_failed: int
    failure_rate: float
    details: Optional[Dict[str, Any]] = None


class ExecutionControl(BaseModel):
    execution_id: str = Field(default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"))
    pipeline_name: str
    start_time: datetime = Field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    status: ExecutionStatus = ExecutionStatus.RUNNING
    input_parameters: Dict[str, Any]
    output_summary: Optional[Dict[str, Any]] = None
    quality_checks: List[DataQualityCheck] = Field(default_factory=list)
    audit_logs: List[AuditLog] = Field(default_factory=list)
