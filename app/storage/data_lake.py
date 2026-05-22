import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from app.utils.logger import StructuredLogger


class DataLake:
    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.logger = StructuredLogger(self.__class__.__name__)

    def resolve_bronze_file_path(self, dataset: str, filename: str) -> Path:
        output_dir = self.base_path / dataset
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / filename

    def write_csv_pages(
        self,
        dataset: str,
        filename: str,
        pages: Iterable[List[Dict[str, Any]]]
    ) -> Tuple[Path, int]:
        file_path = self.resolve_bronze_file_path(dataset, filename)
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
            raise ValueError(f"No records available to write for {dataset}/{filename}")

        self.logger.info(
            "Bronze CSV written",
            dataset=dataset,
            file_path=str(file_path),
            records_count=total_records,
        )
        return file_path, total_records
