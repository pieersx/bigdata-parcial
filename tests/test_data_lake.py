import tempfile
import unittest
from pathlib import Path

from app.storage.data_lake import DataLake


class DataLakeTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.raw_path = self.root / "raw"
        self.bronze_path = self.root / "bronze"
        self.reports_path = self.root / "reports"
        self.data_lake = DataLake(
            raw_path=self.raw_path,
            bronze_path=self.bronze_path,
            reports_path=self.reports_path,
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_existing_input_is_resolved_only_from_raw(self):
        bronze_source = self.bronze_path / "ingresos" / "2012-Ingreso.csv"
        bronze_source.parent.mkdir(parents=True)
        bronze_source.write_text("legacy", encoding="utf-8")

        self.assertIsNone(
            self.data_lake.resolve_existing_raw_path("ingresos", "2012-Ingreso.csv")
        )

        raw_source = self.raw_path / "ingresos" / "2012-Ingreso.csv"
        raw_source.write_text("raw", encoding="utf-8")

        self.assertEqual(
            self.data_lake.resolve_existing_raw_path("ingresos", "2012-Ingreso.csv"),
            raw_source,
        )

    def test_bronze_contract_rejects_source_files(self):
        source_file = self.bronze_path / "sismepre" / "rentas.csv"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("source", encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "Bronze contract violation"):
            self.data_lake.validate_bronze_contract()

    def test_bronze_contract_accepts_parquet_and_spark_metadata(self):
        parquet_file = self.bronze_path / "ingresos" / "year=2026" / "part-00000.parquet"
        parquet_file.parent.mkdir(parents=True)
        parquet_file.write_bytes(b"parquet")
        (parquet_file.parent / "_SUCCESS").write_text("", encoding="utf-8")
        (parquet_file.parent / ".part-00000.parquet.crc").write_text("", encoding="utf-8")

        self.data_lake.validate_bronze_contract()


if __name__ == "__main__":
    unittest.main()
