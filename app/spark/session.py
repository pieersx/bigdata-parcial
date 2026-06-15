from typing import Dict, Optional

from pyspark.sql import SparkSession


class SparkSessionFactory:
    @staticmethod
    def create(config: Optional[Dict[str, str]] = None) -> SparkSession:
        config = config or {}
        builder = (
            SparkSession.builder.appName(config.get("app_name", "bronze-parcial"))
            .config("spark.sql.session.timeZone", config.get("timezone", "UTC"))
            .config(
                "spark.sql.parquet.compression.codec",
                config.get("parquet_compression", "snappy"),
            )
            .config(
                "spark.sql.shuffle.partitions",
                str(config.get("shuffle_partitions", 8)),
            )
            .config("spark.driver.memory", config.get("driver_memory", "4g"))
            .config("spark.executor.memory", config.get("executor_memory", "4g"))
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        )
        return builder.getOrCreate()
