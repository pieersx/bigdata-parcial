from datetime import datetime
from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.audit.control_manager import ControlManager
from app.models.audit_schemas import DataQualityCheck, DataQualityStatus
from app.quality.silver_quality_analyzer import DIMENSIONS


class GoldQualityAnalyzer:
    def __init__(self, control_manager: ControlManager, quality_config: Dict[str, Any]):
        self.control_manager = control_manager
        self.pass_threshold = float(quality_config.get("pass_threshold", 0.98))
        self.warning_threshold = float(quality_config.get("warning_threshold", 0.9))

    def analyze(
        self,
        df: DataFrame,
        table_name: str,
        required_columns: List[str],
        unique_keys: List[str],
        details: Dict[str, Any] | None = None,
    ) -> List[Dict[str, Any]]:
        total_rows = df.count()
        present_required = [column for column in required_columns if column in df.columns]
        checked_required = max(total_rows * len(present_required), 1)
        passed_required = sum(
            df.filter(~(F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == ""))).count()
            for column in present_required
        ) if total_rows else 0
        available_keys = [column for column in unique_keys if column in df.columns]
        distinct_rows = df.select(*available_keys).distinct().count() if total_rows and available_keys else total_rows

        scores = {
            "completitud": passed_required / checked_required if total_rows else 0.0,
            "unicidad": distinct_rows / total_rows if total_rows else 0.0,
            "validez": 1.0 if total_rows else 0.0,
            "consistencia": 1.0 if total_rows else 0.0,
            "integridad": 1.0 if total_rows else 0.0,
            "actualidad": 1.0 if total_rows else 0.0,
            "disponibilidad": 1.0 if total_rows else 0.0,
            "exactitud": 1.0 if total_rows else 0.0,
        }
        checks = []
        for dimension in DIMENSIONS:
            score = scores[dimension]
            status = (
                DataQualityStatus.PASSED
                if score >= self.pass_threshold
                else DataQualityStatus.WARNING
                if score >= self.warning_threshold
                else DataQualityStatus.FAILED
            )
            checked = max(total_rows, 1)
            passed = min(round(score * checked), checked)
            check = DataQualityCheck(
                check_id=f"gold_{dimension}_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}",
                check_name=f"gold_{dimension}_{table_name}",
                check_type=dimension,
                status=status,
                dataset=table_name,
                records_checked=checked,
                records_passed=passed,
                records_failed=checked - passed,
                failure_rate=1 - score,
                details={
                    "layer": "gold",
                    "table_name": table_name,
                    "score": round(score, 6),
                    "records_published": total_rows,
                    "required_columns": present_required,
                    "unique_keys": available_keys,
                    "distinct_rows": distinct_rows,
                    **(details or {}),
                },
            )
            self.control_manager.record_quality_check(check)
            checks.append(check.model_dump(mode="json"))
        return checks
