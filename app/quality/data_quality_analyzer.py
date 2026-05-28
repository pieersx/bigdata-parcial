from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.audit.control_manager import ControlManager
from app.models.audit_schemas import DataQualityCheck, DataQualityStatus
from app.quality.rules_ingresos import DATASET_RULES as INGRESOS_RULES
from app.quality.rules_renamu import DATASET_RULES as RENAMU_RULES
from app.quality.rules_sismepre import DATASET_RULES as SISMEPRE_RULES
from app.utils.logger import StructuredLogger


ALL_RULES = {
    "ingresos": INGRESOS_RULES,
    "sismepre": SISMEPRE_RULES,
    "renamu": RENAMU_RULES,
}


class DataQualityAnalyzer:
    def __init__(self, spark, control_manager: ControlManager, quality_config: Optional[Dict[str, Any]] = None):
        self.spark = spark
        self.control_manager = control_manager
        self.quality_config = quality_config or {}
        self.logger = StructuredLogger(self.__class__.__name__)
        self.pass_threshold = float(self.quality_config.get("pass_threshold", 0.98))
        self.warning_threshold = float(self.quality_config.get("warning_threshold", 0.9))

    def analyze(
        self,
        df: DataFrame,
        dataset: str,
        table_name: str,
        asset: Dict[str, Any],
        bronze_path: Path,
    ) -> List[Dict[str, Any]]:
        if "year" in df.columns:
            year_values = [row["year"] for row in df.select("year").where(F.col("year") != "").distinct().collect()]
        else:
            year_values = [None]

        results: List[Dict[str, Any]] = []
        for year_value in year_values:
            partition_df = df if year_value is None else df.filter(F.col("year") == year_value)
            partition_df = partition_df.cache()
            summary: Dict[str, Any] = {
                "dataset": dataset,
                "table_name": table_name,
                "year": year_value or "all",
                "generated_at": datetime.now().isoformat(),
                "dimensions": [],
            }

            for metric in self._compute_dimension_metrics(partition_df, dataset, table_name, asset, bronze_path, year_value):
                self.control_manager.record_quality_check(metric)
                metric_dump = metric.model_dump(mode="json")
                results.append(metric_dump)
                summary["dimensions"].append(metric_dump)

            metric_name = f"{dataset}_{table_name}_{year_value or 'all'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.control_manager.audit_logger.log_metrics_snapshot(metric_name, summary)
            partition_df.unpersist()

        self.logger.info(
            "Spark data quality analysis completed",
            dataset=dataset,
            table_name=table_name,
            checks_generated=len(results),
        )
        return results

    def _compute_dimension_metrics(
        self,
        df: DataFrame,
        dataset: str,
        table_name: str,
        asset: Dict[str, Any],
        bronze_path: Path,
        year_value: Optional[str],
    ) -> List[DataQualityCheck]:
        rules = ALL_RULES.get(dataset, {}).get(table_name, {})
        total_rows = df.count()

        dimensions = [
            self._completeness_metric(df, dataset, table_name, year_value, rules, total_rows),
            self._uniqueness_metric(df, dataset, table_name, year_value, rules, total_rows),
            self._expression_metric(df, dataset, table_name, year_value, "validez", rules.get("validity_expressions", []), total_rows),
            self._expression_metric(df, dataset, table_name, year_value, "consistencia", rules.get("consistency_expressions", []), total_rows),
            self._integrity_metric(df, dataset, table_name, year_value, rules, bronze_path, total_rows),
            self._expression_metric(df, dataset, table_name, year_value, "actualidad", rules.get("actuality_expressions", []), total_rows, default_score=1.0),
            self._availability_metric(dataset, table_name, year_value, bronze_path, total_rows),
            self._expression_metric(df, dataset, table_name, year_value, "exactitud", rules.get("accuracy_expressions", []), total_rows, default_score=1.0),
        ]
        return [metric for metric in dimensions if metric is not None]

    def _completeness_metric(
        self,
        df: DataFrame,
        dataset: str,
        table_name: str,
        year_value: Optional[str],
        rules: Dict[str, Any],
        total_rows: int,
    ) -> DataQualityCheck:
        required_columns = [column for column in rules.get("required_columns", []) if column in df.columns]
        if not required_columns or total_rows == 0:
            return self._build_check(dataset, table_name, year_value, "completitud", 1, 1, {"required_columns": required_columns})

        passed_units = 0
        checked_units = total_rows * len(required_columns)
        column_scores = {}

        for column_name in required_columns:
            valid_rows = df.filter(~(F.col(column_name).isNull() | (F.trim(F.col(column_name)) == ""))).count()
            passed_units += valid_rows
            column_scores[column_name] = valid_rows / total_rows if total_rows else 1.0

        return self._build_check(
            dataset,
            table_name,
            year_value,
            "completitud",
            checked_units,
            passed_units,
            {"required_columns": required_columns, "column_scores": column_scores},
        )

    def _uniqueness_metric(
        self,
        df: DataFrame,
        dataset: str,
        table_name: str,
        year_value: Optional[str],
        rules: Dict[str, Any],
        total_rows: int,
    ) -> DataQualityCheck:
        unique_keys = [column for column in rules.get("unique_keys", []) if column in df.columns]
        if not unique_keys or total_rows == 0:
            return self._build_check(dataset, table_name, year_value, "unicidad", 1, 1, {"unique_keys": unique_keys})

        distinct_rows = df.select(*unique_keys).distinct().count()
        return self._build_check(
            dataset,
            table_name,
            year_value,
            "unicidad",
            total_rows,
            min(distinct_rows, total_rows),
            {"unique_keys": unique_keys, "distinct_rows": distinct_rows},
        )

    def _integrity_metric(
        self,
        df: DataFrame,
        dataset: str,
        table_name: str,
        year_value: Optional[str],
        rules: Dict[str, Any],
        bronze_path: Path,
        total_rows: int,
    ) -> DataQualityCheck:
        passed_units = 0
        checked_units = 0
        details = {"pairs": {}, "references": {}}

        for code_column, name_column in rules.get("integrity_pairs", []):
            if code_column not in df.columns or name_column not in df.columns or total_rows == 0:
                continue
            pair_passed = df.filter(
                (F.trim(F.col(code_column)) == "") | (~(F.col(name_column).isNull() | (F.trim(F.col(name_column)) == "")))
            ).count()
            checked_units += total_rows
            passed_units += pair_passed
            details["pairs"][f"{code_column}->{name_column}"] = pair_passed / total_rows if total_rows else 1.0

        for reference_rule in rules.get("integrity_references", []):
            reference_table = reference_rule["reference_table"]
            reference_path = self._resolve_reference_path(dataset, reference_table, bronze_path)
            if not reference_path.exists() or total_rows == 0:
                continue

            reference_df = self.spark.read.parquet(str(reference_path))
            join_columns: List[Tuple[str, str]] = reference_rule["join_columns"]
            available_join_columns = [pair for pair in join_columns if pair[0] in df.columns and pair[1] in reference_df.columns]
            if not available_join_columns:
                continue

            source_alias = df.alias("src")
            reference_alias = reference_df.alias("ref")
            join_condition = [F.col(f"src.{src}") == F.col(f"ref.{ref}") for src, ref in available_join_columns]
            matched_rows = source_alias.join(reference_alias, on=join_condition, how="left_semi").count()
            checked_units += total_rows
            passed_units += matched_rows
            details["references"][reference_table] = {
                "join_columns": available_join_columns,
                "match_ratio": matched_rows / total_rows if total_rows else 1.0,
            }

        if checked_units == 0:
            return self._build_check(dataset, table_name, year_value, "integridad", 1, 1, details)
        return self._build_check(dataset, table_name, year_value, "integridad", checked_units, passed_units, details)

    def _availability_metric(
        self,
        dataset: str,
        table_name: str,
        year_value: Optional[str],
        bronze_path: Path,
        total_rows: int,
    ) -> DataQualityCheck:
        available = bronze_path.exists() and total_rows > 0
        return self._build_check(
            dataset,
            table_name,
            year_value,
            "disponibilidad",
            1,
            1 if available else 0,
            {"bronze_path": str(bronze_path), "records_count": total_rows},
        )

    def _expression_metric(
        self,
        df: DataFrame,
        dataset: str,
        table_name: str,
        year_value: Optional[str],
        dimension: str,
        expressions: List[Dict[str, str]],
        total_rows: int,
        default_score: float = 1.0,
    ) -> DataQualityCheck:
        if not expressions or total_rows == 0:
            checked_units = 1
            passed_units = int(round(default_score))
            return self._build_check(dataset, table_name, year_value, dimension, checked_units, passed_units, {"rules": expressions})

        passed_units = 0
        checked_units = total_rows * len(expressions)
        rule_scores = {}

        for expression_rule in expressions:
            rule_name = expression_rule["name"]
            rule_expression = expression_rule["expr"]
            valid_rows = df.filter(F.expr(rule_expression)).count()
            passed_units += valid_rows
            rule_scores[rule_name] = {"expr": rule_expression, "score": valid_rows / total_rows if total_rows else 1.0}

        return self._build_check(dataset, table_name, year_value, dimension, checked_units, passed_units, {"rules": rule_scores})

    def _resolve_reference_path(self, dataset: str, reference_table: str, bronze_path: Path) -> Path:
        if dataset == "sismepre":
            return bronze_path.parent / reference_table
        if dataset == "ingresos":
            return bronze_path / "_references" / reference_table
        return bronze_path.parent / reference_table

    def _build_check(
        self,
        dataset: str,
        table_name: str,
        year_value: Optional[str],
        dimension: str,
        checked_units: int,
        passed_units: int,
        details: Dict[str, Any],
    ) -> DataQualityCheck:
        checked_units = max(checked_units, 1)
        passed_units = min(max(passed_units, 0), checked_units)
        score = passed_units / checked_units
        failure_rate = 1 - score
        if score >= self.pass_threshold:
            status = DataQualityStatus.PASSED
        elif score >= self.warning_threshold:
            status = DataQualityStatus.WARNING
        else:
            status = DataQualityStatus.FAILED

        year_suffix = year_value or "all"
        check_name = f"{dimension}_{dataset}_{table_name}_{year_suffix}"
        check_id = f"{check_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        return DataQualityCheck(
            check_id=check_id,
            check_name=check_name,
            check_type=dimension,
            status=status,
            dataset=dataset,
            records_checked=checked_units,
            records_passed=passed_units,
            records_failed=checked_units - passed_units,
            failure_rate=failure_rate,
            details={
                "dataset": dataset,
                "table_name": table_name,
                "year": year_suffix,
                "score": round(score, 6),
                **details,
            },
        )
