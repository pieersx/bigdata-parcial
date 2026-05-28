from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import DataFrame

from app.storage.data_lake import DataLake
from app.utils.logger import StructuredLogger


class ParquetStorage:
    def __init__(self, data_lake: DataLake):
        self.data_lake = data_lake
        self.logger = StructuredLogger(self.__class__.__name__)

    def write(
        self,
        df: DataFrame,
        dataset: str,
        table_name: str,
        asset_role: str,
        partition_columns: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        partition_columns = partition_columns or []
        output_path = self.data_lake.resolve_bronze_table_path(dataset, table_name, asset_role=asset_role)

        writer = df.write.mode("overwrite")
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.parquet(str(output_path))

        self.logger.info(
            "Bronze parquet written",
            dataset=dataset,
            table_name=table_name,
            asset_role=asset_role,
            output_path=str(output_path),
            partition_columns=partition_columns,
        )

        return {
            "bronze_path": str(output_path),
            "format": "parquet",
            "partition_columns": ",".join(partition_columns) if partition_columns else "",
        }
