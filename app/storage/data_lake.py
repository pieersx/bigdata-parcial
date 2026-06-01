import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.utils.logger import StructuredLogger


class DataLake:
    def __init__(
        self,
        raw_path: Path,
        bronze_path: Path,
        reports_path: Path,
        silver_path: Optional[Path] = None,
    ):
        self.raw_path = raw_path
        self.bronze_path = bronze_path
        self.reports_path = reports_path
        self.silver_path = silver_path or bronze_path.parent / "silver"
        self.logger = StructuredLogger(self.__class__.__name__)

        for path in (self.raw_path, self.bronze_path, self.silver_path, self.reports_path):
            path.mkdir(parents=True, exist_ok=True)

    def resolve_raw_file_path(self, dataset: str, filename: str) -> Path:
        output_dir = self.raw_path / dataset
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / filename

    def resolve_existing_raw_path(self, dataset: str, filename: str) -> Optional[Path]:
        raw_candidate = self.resolve_raw_file_path(dataset, filename)
        if raw_candidate.exists() and raw_candidate.stat().st_size > 0:
            return raw_candidate
        return None

    def validate_bronze_contract(self) -> None:
        source_extensions = {".csv", ".json", ".pdf", ".zip"}
        unexpected_files = sorted(
            path.relative_to(self.bronze_path)
            for path in self.bronze_path.rglob("*")
            if path.is_file() and path.suffix.lower() in source_extensions
        )
        if unexpected_files:
            formatted_paths = ", ".join(str(path) for path in unexpected_files[:10])
            remaining_count = len(unexpected_files) - 10
            if remaining_count > 0:
                formatted_paths += f", ... and {remaining_count} more"
            raise ValueError(
                "Bronze contract violation: source files must live under raw, "
                f"not bronze. Unexpected files: {formatted_paths}"
            )

    def resolve_bronze_table_path(
        self,
        dataset: str,
        table_name: str,
        asset_role: str = "table",
    ) -> Path:
        dataset_dir = self.bronze_path / dataset
        dataset_dir.mkdir(parents=True, exist_ok=True)

        if asset_role == "reference":
            output_dir = dataset_dir / "_references" / table_name
        elif dataset == "sismepre" and table_name != dataset:
            output_dir = dataset_dir / table_name
        else:
            output_dir = dataset_dir

        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def resolve_reports_path(self, dataset: str, table_name: Optional[str] = None) -> Path:
        output_dir = self.reports_path / dataset
        if table_name and table_name not in (dataset, ""):
            output_dir = output_dir / table_name
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def resolve_silver_table_path(self, table_name: str) -> Path:
        output_dir = self.silver_path / table_name
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def resolve_quarantine_path(self, table_name: str) -> Path:
        output_dir = self.silver_path / "_quarantine" / table_name
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def write_raw_csv_pages(
        self,
        dataset: str,
        filename: str,
        pages: Iterable[List[Dict[str, Any]]],
    ) -> Tuple[Path, int]:
        file_path = self.resolve_raw_file_path(dataset, filename)
        total_records = 0
        header_written = False
        writer = None

        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as file_handle:
                for records in pages:
                    if not records:
                        continue

                    if not header_written:
                        fieldnames = list(records[0].keys())
                        writer = csv.DictWriter(
                            file_handle,
                            fieldnames=fieldnames,
                            extrasaction='ignore',
                        )
                        writer.writeheader()
                        header_written = True

                    writer.writerows(records)
                    total_records += len(records)
        except Exception:
            if file_path.exists():
                file_path.unlink()
            raise

        if not header_written:
            if file_path.exists():
                file_path.unlink()
            raise ValueError(f"No records available to write for raw/{dataset}/{filename}")

        self.logger.info(
            "Raw CSV written",
            dataset=dataset,
            file_path=str(file_path),
            records_count=total_records,
        )
        return file_path, total_records
