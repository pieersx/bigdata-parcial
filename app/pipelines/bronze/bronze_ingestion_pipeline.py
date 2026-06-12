from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.audit.control_manager import ControlManager
from app.services.bronze_transform_service import BronzeTransformService
from app.services.landing_service import LandingService
from app.utils.logger import StructuredLogger


class BronzeIngestionPipeline(ABC):
    dataset_name: str

    def __init__(
        self,
        landing_service: LandingService,
        bronze_transform_service: BronzeTransformService,
        dataset_config: Dict[str, Any],
        control_manager: Optional[ControlManager] = None,
    ):
        self.landing_service = landing_service
        self.bronze_transform_service = bronze_transform_service
        self.dataset_config = dataset_config
        self.control_manager = control_manager
        self.logger = StructuredLogger(self.__class__.__name__)

    def run(self) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for asset in self.build_assets():
            landing_result = self.landing_service.fetch(self.dataset_name, asset)
            if landing_result["status"] == "error":
                results.append(landing_result)
                continue

            if landing_result["status"] == "skipped_optional" or not landing_result.get("raw_path"):
                results.append(landing_result)
                continue

            if not asset.get("bronze_enabled", True):
                results.append(landing_result)
                continue

            try:
                results.append(
                    self.bronze_transform_service.transform(
                        dataset=self.dataset_name,
                        asset=asset,
                        landing_result=landing_result,
                        execution_id=(
                            self.control_manager.current_execution.execution_id
                            if self.control_manager and self.control_manager.current_execution
                            else None
                        ),
                    )
                )
            except Exception as error:
                self.logger.error(
                    "Bronze transformation failed",
                    dataset=self.dataset_name,
                    asset_name=asset["name"],
                    error=str(error),
                )
                if self.control_manager:
                    self.control_manager.log_pipeline_error(
                        error_type="BronzeTransformError",
                        error_message=str(error),
                        context={"dataset": self.dataset_name, "asset_name": asset["name"]},
                    )
                results.append(
                    {
                        **landing_result,
                        "status": "error",
                        "error_message": str(error),
                    }
                )

        return results

    @abstractmethod
    def build_assets(self) -> List[Dict[str, Any]]:
        pass

    def _csv_asset(
        self,
        *,
        name: str,
        filename: str,
        url: str,
        table_name: str,
        asset_role: str,
        source_type: str,
        year_column: Optional[str] = None,
        default_year: Optional[int] = None,
        required: bool = True,
        profiling_enabled: bool = False,
        quality_enabled: bool = True,
        validate_last_modified: bool = False,
    ) -> Dict[str, Any]:
        return {
            "name": name,
            "filename": filename,
            "url": url,
            "table_name": table_name,
            "asset_role": asset_role,
            "source_type": source_type,
            "read_options": self.dataset_config.get("read_options", {}),
            "year_column": year_column,
            "default_year": default_year,
            "required": required,
            "profiling_enabled": profiling_enabled,
            "quality_enabled": quality_enabled,
            "bronze_enabled": True,
            "validate_last_modified": validate_last_modified,
        }
