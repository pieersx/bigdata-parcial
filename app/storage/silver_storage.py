from pathlib import Path
import shutil
from typing import Dict, List, Optional

from pyspark.sql import DataFrame

from app.storage.data_lake import DataLake
from app.utils.logger import StructuredLogger


class SilverStorage:
    def __init__(self, data_lake: DataLake, write_mode: str = "overwrite"):
        self.data_lake = data_lake
        self.write_mode = write_mode
        self.logger = StructuredLogger(self.__class__.__name__)

    def write_table(
        self,
        df: DataFrame,
        table_name: str,
        partition_columns: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        return self._write(df, self.data_lake.resolve_silver_table_path(table_name), table_name, partition_columns)

    def write_quarantine(
        self,
        df: DataFrame,
        table_name: str,
        partition_columns: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        return self._write(df, self.data_lake.resolve_quarantine_path(table_name), table_name, partition_columns)

    def clear_table(self, table_name: str) -> None:
        self._clear(self.data_lake.silver_path / table_name)

    def clear_quarantine(self, table_name: str) -> None:
        self._clear(self.data_lake.silver_path / "_quarantine" / table_name)

    def _write(
        self,
        df: DataFrame,
        output_path: Path,
        table_name: str,
        partition_columns: Optional[List[str]],
    ) -> Dict[str, str]:
        partition_columns = [column for column in (partition_columns or []) if column in df.columns]
        writer = df.write.mode(self.write_mode)
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.parquet(str(output_path))
        self.logger.info(
            "Silver parquet written",
            table_name=table_name,
            output_path=str(output_path),
            partition_columns=partition_columns,
        )
        return {
            "path": str(output_path),
            "format": "parquet",
            "partition_columns": ",".join(partition_columns),
        }

    def _clear(self, output_path: Path) -> None:
        silver_root = self.data_lake.silver_path.resolve()
        resolved_path = output_path.resolve()
        if not resolved_path.is_relative_to(silver_root) or resolved_path == silver_root:
            raise ValueError(f"Refusing to clear Silver path outside its root: {resolved_path}")
        if resolved_path.exists():
            shutil.rmtree(resolved_path)
            self.logger.info("Stale Silver output cleared", output_path=str(resolved_path))
